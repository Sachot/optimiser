/**
 * 
 */
package sjdb;

/**
 * @author nmg
 * 一元操作符，只会有一个child
 *
 */
public abstract class UnaryOperator extends Operator {
	/**
	 * 
	 */
	public UnaryOperator(Operator input) {
		super();	
		this.inputs.add(input);
		this.output = null;
	}

	/**
	 * Return the single child operator of this operator
	 * @return Child operator
	 */
	public Operator getInput() {
		return this.inputs.get(0);
	}

	/* (non-Javadoc)
	 * @see sjdb.Operator#accept(sjdb.OperatorVisitor)
	 */
	public void accept(PlanVisitor visitor) {
		super.accept(visitor);
		// depth-first traversal - accept the 
	}
}
