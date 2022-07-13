/**
 * 
 */
package sjdb;

/**
 * @author nmg
 * 选择运算符
 *
 */
public class Select extends UnaryOperator {
	private Predicate predicate;
	
	/**
	 * @param input
	 */
	public Select(Operator input, Predicate predicate) {
		super(input);
		this.predicate = predicate;
	}

	public Predicate getPredicate() {
		return this.predicate;
	}
	
	public void accept(PlanVisitor visitor) {
		// 通过递归，实现查询树后续遍历
		super.accept(visitor);
		// 处理当前节点
		visitor.visit(this);
	}
	
	public String toString() {
		return "SELECT [" + this.predicate.toString() + "] (" + getInput().toString() + ")";
	}
}
