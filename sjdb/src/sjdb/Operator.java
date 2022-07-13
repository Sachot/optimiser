/**
 * 
 */
package sjdb;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * This abstract class represents an operator in a query, and
 * is subclassed by UnaryOperator and BinaryOperator.
 * @author nmg
 *
 */
public abstract class Operator {
	/**
	 * The list of child operators that feed their outputs to
	 * this operator.
	 */
	protected ArrayList<Operator> inputs;
	/**
	 * The relation produced by this operator as output.
	 */
	protected Relation output;
	
	public Operator() {
		this.inputs = new ArrayList<Operator>();
	}
	
	/**
	 * Return an arraylist containing the child operators of this
	 * operator.
	 * @return Child operators
	 */
	public List<Operator> getInputs() {
		List<Operator> inputs = new ArrayList<Operator>();
		inputs.addAll(this.inputs);
		return inputs;
	}
	
	/**
	 * Add a child operator to this operator
	 * @param op Child operator
	 */
	protected void addOperator(Operator op) {
		this.inputs.add(op);
	}
	
	/**
	 * Return the relation produced by this operator as output.
	 * @return Output relation
	 */
	public Relation getOutput() {
		return this.output;
	}
	
	/**
	 * Set the relation produced by this operator as output.
	 * Relation有Attribute和size，size是output的tuple数量
	 * @param reln Output relation
	 */
	public void setOutput(Relation reln) {
		this.output = reln;
	}
	
	/**
	 * Accept a visitor to this operator.
	 * @param visitor Visitor to be accepted
	 */
	public void accept(PlanVisitor visitor) {
		// 注意这里是取当前operator里面的input中的operator，不包括当前operator，目的是实现查询树后续遍历，从树的底部开始执行查询
		Iterator<Operator> iter = this.inputs.iterator();
		while (iter.hasNext()){
			iter.next().accept(visitor);
		}
	}
}
