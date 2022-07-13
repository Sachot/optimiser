package sjdb;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

public class Estimator implements PlanVisitor {
	private int cost = 0;
	public Estimator() {
		// empty constructor
	}

	/**
	 * 在一颗查询树中，每个op有input和output；
	 * input记录着从查询树底端到现在执行过的op记录；
	 * output记录着处理后的结果，即处理完以后的会有多少个tuple，哪些attribute。
	 *
	 * 在这个查询优化器作业中，我们假设数据平均分布；
	 * 设关系R(R表)中，tuple数量为T(R)；属性A在R表中有多少个不同值，记为V(R,A)，因此一定满足V(R,A)<=T(R)；
	 * 每一步操作我们都会累加到目前为止所有output的tuple总数，以tuple数量来衡量开销大小，选择累计tuple数量最少的方案
	 *
	 * scan 扫描操作: 读取下方传来的所有tuple，输出T(R)=输入T(R)
	 * product 乘积操作: 相当于算笛卡尔积，输出T(RxS) = 输入T(R)T(S)
	 * projection 投射操作: 从一组attribute中选取部分attribute，相当于把tuple中一些不需要的属性给过滤掉，输出T(R)=输入T(R)
	 * select 选择操作: 分为两类，一种是例如user.name=ZhixuanCai，另一种是user.id = file.user，前者是SQL里面的select内容，后者实际上是inner join
	 * 对于attribute=val，有:T(select(R)) = T(R)/V(R,A), V(select(R), A) = 1
	 *
	 * 对于join，即attribute=attribute，有:
	 * T(join(R,S,A=B)) = T(R)T(S) / Max(V(R,A), V(S,B))
	 * 这个不太好理解，可以举个例子:
	 * 假设我们需要join表1和表2 on T1.A=T2.B；表1有6个tuple，属性A有3个不同值；表2有4个tuple，属性B有2个不同值；如果B中的两个值都能在A中找到，我们join后会得到8个tuple，即6*4/3
	 *
	 * V(join(R,S,A=B), A) = V(join(R,S,A=B), B) = Min(V(R,A), V(S,B))
	 *
	 * for an attribute C of R that is not a join attribute: V(join(R,S,A=B), C) = V(R, C)
	 */

	/* 
	 * Create output relation on Scan operator
	 *
	 * Example implementation of visit method for Scan operators.
	 */
	public void visit(Scan op) {
		// scan all attributes in a table
		Relation input = op.getRelation();
		Relation output = new Relation(input.getTupleCount());
		
		Iterator<Attribute> iter = input.getAttributes().iterator();
		while (iter.hasNext()) {
			output.addAttribute(new Attribute(iter.next()));
		}
		cost += output.getTupleCount();
		op.setOutput(output);
	}

	/*
	 * selector the specific attributes from the input-operator's output
	 */
	public void visit(Project op) {
		Relation input = op.getInput().getOutput();
		List<Attribute> inputAttributes = input.getAttributes();
		// the tuple count is unchanged
		Relation output = new Relation(input.getTupleCount());
		Iterator<Attribute> iter = op.getAttributes().iterator();
		while(iter.hasNext()) {
			/*Attribute a = iter.next();
			if(inputAttributes.contains(a)) {
				output.addAttribute(new Attribute(a));
			}*/
			output.addAttribute(new Attribute(input.getAttribute(iter.next())));
		}
		cost += output.getTupleCount();
		op.setOutput(output);
	}

	/*
	 * unary
	 * case 1: attr=val
	 * T(select(R)) = T(R)/V(R,A), V(select(R), A) = 1
	 *
	 * case 2: attr=attr
	 * T(select(R)) = T(R)/Max(V(R,A), V(R,B)),
	 * V(select(R), A) = V(select(R), B) = Min(V(R,A), V(R,B))
	 */
	public void visit(Select op) {
		Relation input = op.getInput().getOutput();
		Predicate predicate = op.getPredicate();
		List<Attribute> inputAttr = input.getAttributes();
		Relation output;
		int selectCount = 0;  // T(select(R))
		int value = 0;        // V(select(R), A)
		int inputRelationNum = input.getTupleCount();  // T(R)
		Attribute leftAttr = input.getAttribute(predicate.getLeftAttribute());  // V(R,A)
		Attribute rightAttr = null;   // V(R,B)

		if(predicate.equalsValue()) {
			// case 1
			if((inputRelationNum % leftAttr.getValueCount())==0) {
				selectCount = inputRelationNum / leftAttr.getValueCount();
			}else {
				selectCount = (inputRelationNum / leftAttr.getValueCount())+1;
			}
			value = 1;
		}else {
			// case 2
			rightAttr = input.getAttribute(predicate.getRightAttribute());
			int leftCount = leftAttr.getValueCount();
			int rightCount = rightAttr.getValueCount();
			int count = Math.max(leftCount, rightCount);
			if((inputRelationNum % count)==0) {
				selectCount = inputRelationNum / count;
			}else {
				selectCount = (inputRelationNum / count)+1;
			}
			value = Math.min(leftCount, rightCount);
		}

		output = new Relation(selectCount);
		Iterator<Attribute> iter = inputAttr.iterator();
		while(iter.hasNext()) {
			Attribute a = iter.next();
			if(a.equals(leftAttr) || a.equals(rightAttr)) {
				output.addAttribute(new Attribute(a.getName(), value));
			}else {
				output.addAttribute(new Attribute(a));
			}
		}
		cost += output.getTupleCount();
		op.setOutput(output);
	}

	/*
	 * binary
	 * T(RxS) = T(R)T(S)
	 */
	public void visit(Product op) {
		Relation leftInput = op.getLeft().output;
		Relation rightInput = op.getRight().output;
		Relation output = new Relation(leftInput.getTupleCount()* rightInput.getTupleCount());

		Iterator<Attribute> leftIter = leftInput.getAttributes().iterator();
		Iterator<Attribute> rightIter = rightInput.getAttributes().iterator();
		while(leftIter.hasNext()) {
			Attribute a = leftIter.next();
			output.addAttribute(new Attribute(a));
		}
		while(rightIter.hasNext()) {
			Attribute a = rightIter.next();
			output.addAttribute(new Attribute(a));
		}
		cost += output.getTupleCount();
		op.setOutput(output);
	}

	/*
	 * binary
	 * the predicate in Join must be "attr=attr"
	 * T(join(R,S,A=B)) = T(R)T(S) / Max(V(R,A), V(S,B))
	 * V(join(R,S,A=B), A) = V(join(R,S,A=B), B) = Min(V(R,A), V(S,B))
	 * for an attribute C of R that is not a join attribute: V(join(R,S,A=B), C) = V(R, C)
	 */
	public void visit0(Join op) {
		Relation leftInput = op.getLeft().getOutput();
		Relation rightInput = op.getRight().getOutput();
		Predicate predicate = op.getPredicate();
		Relation output;
		int selectCount = 0;   // T(join(R,S,A=B))
		int joinValue = 0;     // V(join(R,S,A=B), A)
		int leftInputTupleNum = leftInput.getTupleCount();    // T(R)
		int rightInputTupleNum = rightInput.getTupleCount();  // T(S)
		int leftAttrValNum = leftInput.getAttribute(predicate.getLeftAttribute()).getValueCount();    // V(R,A)
		int rightAttrValNum = rightInput.getAttribute(predicate.getRightAttribute()).getValueCount(); // V(S,B)
		Iterator<Attribute> leftIter = leftInput.getAttributes().iterator();
		Iterator<Attribute> rightIter = rightInput.getAttributes().iterator();

		selectCount = (int)Math.ceil((double)((leftInputTupleNum*rightInputTupleNum) / Math.max(leftAttrValNum, rightAttrValNum)));
		joinValue = Math.min(leftAttrValNum, rightAttrValNum);
		output = new Relation(selectCount);

		while(leftIter.hasNext()) {
			Attribute a = leftIter.next();
			if(a.equals(predicate.getLeftAttribute())) {
				output.addAttribute(new Attribute(a.getName(), joinValue));
			}else {
				output.addAttribute(new Attribute(a));
			}
		}
		while(rightIter.hasNext()) {
			Attribute a = rightIter.next();
			if(a.equals(predicate.getRightAttribute())) {
				output.addAttribute(new Attribute(a.getName(), joinValue));
			}else {
				output.addAttribute(new Attribute(a));
			}
		}
		cost += output.getTupleCount();
		op.setOutput(output);
	}
	public void visit(Join op) {
		int max_count, min_count;
		Relation LeftInput = op.getLeft().getOutput();
		Relation RightInput = op.getRight().getOutput();

		//get the predicate attributes and input relations attributes
		Predicate predicate = op.getPredicate();

		Attribute leftAttribute = predicate.getLeftAttribute();
		Attribute LeftAttr = LeftInput.getAttribute(leftAttribute);

		Attribute rightAttribute = predicate.getRightAttribute();
		Attribute RightAttr = RightInput.getAttribute(rightAttribute);

		//calculate the tuple count and generate the output based on that
		max_count = Math.max(LeftAttr.getValueCount(), RightAttr.getValueCount());
		Relation output = new Relation((LeftInput.getTupleCount() * RightInput.getTupleCount())/max_count);

		//attributes with initial relations
		//with some changes to the value counts
		Iterator<Attribute> Leftiter = LeftInput.getAttributes().iterator();
		min_count = Math.min(LeftAttr.getValueCount(), RightAttr.getValueCount());
		while (Leftiter.hasNext()) {
			Attribute attr = new Attribute(Leftiter.next());
			if(attr.equals(LeftAttr))
				output.addAttribute(new Attribute(attr.getName(), min_count));
			else
				output.addAttribute(attr);
		}

		Iterator<Attribute> Rightiter = RightInput.getAttributes().iterator();
		while (Rightiter.hasNext()) {
			Attribute attr = new Attribute(Rightiter.next());
			if(attr.equals(RightAttr))
				output.addAttribute(new Attribute(attr.getName(), Math.min(LeftAttr.getValueCount(), RightAttr.getValueCount())));
			else
				output.addAttribute(attr);
		}
		op.setOutput(output);
		//count the query cost for selection
		cost = cost + output.getTupleCount();
	}

	/*
	 * estimate the cost of this plan
	 */
	public int estimate(Operator plan) {
		this.cost = 0;
		plan.accept(this);
		return this.cost;
	}
}
