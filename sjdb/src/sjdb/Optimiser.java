package sjdb;

import javax.naming.OperationNotSupportedException;
import java.util.*;

public class Optimiser implements PlanVisitor{
    private Catalogue catalogue;
    private Estimator estimator = new Estimator();
    private Operator oriPlan;
    private Set<Scan> scans = new HashSet<>();
    private Set<Predicate> valPres = new HashSet<>();
    private Set<Predicate> attrPres = new HashSet<>();
    private Set<Attribute> allAttrs = new HashSet<>();
    private Set<Predicate> allPres = new HashSet<>();

    // 全排列 枚举left join的所有情况
    private boolean[] used;
    private Predicate[] arr;
    private List<Predicate> tracking = new ArrayList<>();
    private List<List<Predicate>> pmtRes = new ArrayList<List<Predicate>>();

    public Optimiser(Catalogue catalogue) {
        this.catalogue = catalogue;
    }
    // 沿用项目的访问者设计模式 - visitor pattern
    @Override
    public void visit(Scan op) {
        // already has data in output
        scans.add(new Scan((NamedRelation) op.getRelation()));
    }
    // 把所有投射的attr都放入到allAttrs中
    @Override
    public void visit(Project op) {
        allAttrs.addAll(op.getAttributes());
    }
    // select操作中所有的attr也放到allAttrs中，并分别记录有哪些predicate是attr=val，哪些是attr=attr的，后者用于生成join
    @Override
    public void visit(Select op) {
        allPres.add(op.getPredicate());
        if(op.getPredicate().equalsValue()) {
            valPres.add(op.getPredicate());
            allAttrs.add(op.getPredicate().getLeftAttribute());
        }else {
            attrPres.add(op.getPredicate());
            allAttrs.add(op.getPredicate().getLeftAttribute());
            allAttrs.add(op.getPredicate().getRightAttribute());
        }
    }
    // 全连接操作会被优化取缔
    @Override
    public void visit(Product op) {
        return;
    }

    // 因为优化前没有join操作，所以不需要处理
    @Override
    public void visit(Join op) {
        return;
    }


    public Operator optimise(Operator plan) {
        plan.accept(this);
        this.oriPlan = plan;
        List<Operator> selectRes = moveSelectionDown();
        // now all the predicates in attrPres are from different relations
        List<Operator> productRes = moveProjectDown(selectRes);
        List<List<Predicate>> allPmt = fullPermute(this.attrPres);
        // find the best join order
        Operator bestPlan = joinReorder(allPmt, productRes);
        if(this.oriPlan instanceof Project) {
            bestPlan = new Project(bestPlan, ((Project) this.oriPlan).getAttributes());
            bestPlan.accept(estimator);
        }
        return bestPlan;
    }

    /**
     * 把所有attr=val都往下推到这一步进行，目的是为了尽可能减少需要向上传递的tuple数量
     * @return 返回所有relation经过select(attr=val)的查询树的节点集合
     */
    private List<Operator> moveSelectionDown() {
        List<Operator> res = new ArrayList<>();
        Operator op;
        // scans是表集合，遍历relation
        for(Scan s : scans) {
            // scans have data in s.output
            op = s;
            List<Attribute> attrs = op.getOutput().getAttributes();
            Iterator<Predicate> valIt = valPres.iterator();
            Iterator<Predicate> attrIt = attrPres.iterator();
            while(valIt.hasNext()) {
                Predicate p = valIt.next();
                // produce output，如果这个operator的output为空，说明这是新生成的operator，执行estimate.visit()
                if(op.getOutput()==null) {
                    op.accept(estimator);
                }
                // 说明找到这个relation下的attr=val，把select(attr=val)下推到这里生成，新生成的operator会在循环执行开销评估
                // select step by step, once calculate one attribute's tuple count
                if(attrs.contains(p.getLeftAttribute())) {
                    op = new Select(op, p);
                }
            }
            while(attrIt.hasNext()) {
                // for both of two attrs are from the same relation
                Predicate p = attrIt.next();
                // 如果这个operator的output为空，说明这是新生成的operator，执行estimate.visit()
                if(op.getOutput()==null) {
                    op.accept(estimator);
                }
                // 主要是为了防止attr1=attr2中，两个attr都来自一个relation的情况，出现这种情况，查询开销不需要在join部分处理，这里可以直接就用这一个relation中算开销
                // 但需要注意的是，这里可能与自连接的逻辑相违背（self join是把自己看成两张不同的表处理）
                if(attrs.contains(p.getLeftAttribute()) && attrs.contains(p.getRightAttribute())) {
                    op = new Select(op, p);
                    // remove self selection to make sure that all the predicates in attrPres are from different relations
                    attrIt.remove();
                }
            }
            if(op.getOutput()==null) {
                op.accept(estimator);
            }
            res.add(op);
        }
        return res;
    }

    private List<Operator> moveProjectDown(List<Operator> selectRes) {
        List<Operator> res = new ArrayList<>();
        // set contains all attributes in a specific relation
        Set<Attribute> set = new HashSet<>();
        Iterator<Predicate> attrIt = attrPres.iterator();
        while(attrIt.hasNext()) {
            Predicate p = attrIt.next();
            set.add(p.getLeftAttribute());
            set.add(p.getRightAttribute());
        }
        // 因为投射操作一定在最外层，即原始查询树的根节点，
        // 把投射操作涉及到的全部attribute都放入set中，此时set包含了这次查询任务中涉及到的全部attribute(join和投射所用到的attr)
        if(this.oriPlan instanceof Project) {
            set.addAll(((Project) this.oriPlan).getAttributes());
        }
        Iterator<Operator> inputIt = selectRes.iterator();
        while(inputIt.hasNext()) {
            Operator op = inputIt.next();
            // get the intersection of two sets
            Set<Attribute> copySet = new HashSet<>();
            copySet.addAll(set);
            // 让copySet中的attribute均为这个op所对应的relation的attribute
            copySet.retainAll(op.getOutput().getAttributes());
            if(!copySet.isEmpty()) {
                // 实现投射下推
                List<Attribute> l = new ArrayList<>(copySet);
                Operator project = new Project(op, l);
                project.accept(estimator);
                res.add(project);
            }else {
                res.add(op);
            }
        }
        return res;
    }

    private Operator joinReorder(List<List<Predicate>> presList, List<Operator> productRes) {
        Operator bestOperator = null;
        int lowestCost = Integer.MAX_VALUE;
        // no join
        if(productRes.size()==1) {
            bestOperator = productRes.get(0);
            bestOperator.accept(estimator);
            return bestOperator;
        }

        for(List<Predicate> list : presList) {
            Predicate predicate = list.get(0);
            Attribute leftAttr = predicate.getLeftAttribute();
            Attribute rightAttr = predicate.getRightAttribute();
            Operator leftOp = null;
            Operator rightOp = null;
            boolean f1 = false;
            boolean f2 = false;
            List<Operator> copyList = new ArrayList<>();
            copyList.addAll(productRes);
            Iterator<Operator> it = copyList.iterator();
            while(it.hasNext()) {
                if(f1&&f2) {
                    break;
                }
                Operator o = it.next();
                if(!f1 && o.getOutput().getAttributes().contains(leftAttr)) {
                    leftOp = o;
                    it.remove();
                    f1 = true;
                    continue;
                }
                if(!f2 && o.getOutput().getAttributes().contains(rightAttr)) {
                    rightOp = o;
                    it.remove();
                    f2 = true;
                }
            }

            Operator join1 = new Join(leftOp, rightOp, predicate);
            join1.accept(estimator);

            list.remove(0);
            // only 1 join
            if(list.isEmpty()) {
                bestOperator = join1;
                return bestOperator;
            }

            // more than 1 join
            Operator lastJoin = join1;
            Iterator<Predicate> preIt = list.iterator();
            while(preIt.hasNext()) {
                Predicate p = preIt.next();
                Attribute l = p.getLeftAttribute();
                Attribute r = p.getRightAttribute();
                if(lastJoin.getOutput().getAttributes().contains(l)) {
                    Iterator<Operator> copyIt = copyList.iterator();
                    while(copyIt.hasNext()) {
                        Operator op = copyIt.next();
                        if(op.getOutput().getAttributes().contains(r)) {
                            lastJoin = new Join(lastJoin, op, p);
                            lastJoin.accept(estimator);
                            copyIt.remove();
                            break;
                        }
                    }
                }else if(lastJoin.getOutput().getAttributes().contains(r)) {
                    Iterator<Operator> copyIt = copyList.iterator();
                    while(copyIt.hasNext()) {
                        Operator op = copyIt.next();
                        if(op.getOutput().getAttributes().contains(l)) {
                            lastJoin = new Join(lastJoin, op, p);
                            lastJoin.accept(estimator);
                            copyIt.remove();
                            break;
                        }
                    }
                }
            }
            int cost = estimator.estimate(lastJoin);
            if(cost < lowestCost) {
                lowestCost = cost;
                bestOperator = lastJoin;
            }
        }
        return bestOperator;
    }

    private List<List<Predicate>> fullPermute(Set<Predicate> pres) {
        this.arr = new Predicate[pres.size()];
        Iterator<Predicate> it = pres.iterator();
        int i=0;
        while(it.hasNext()) {
            Predicate p = it.next();
            this.arr[i] = p;
            i++;
        }
        this.used = new boolean[pres.size()];
        backTrack();
        return pmtRes;
    }

    private void backTrack() {
        if(tracking.size()== arr.length) {
            pmtRes.add(new ArrayList<>(tracking));
        }
        for(int i=0;i< arr.length;i++) {
            if(used[i]) {
                continue;
            }
            used[i] = true;
            tracking.add(arr[i]);
            backTrack();
            tracking.remove(tracking.size()-1);
            used[i] = false;
        }
        return;
    }
}
