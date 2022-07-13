package sjdb;

import java.util.List;
import java.util.Iterator;

/**
 * This class represents a Project operator.
 * 投射操作（选出指定Attribute）
 * project属于一元操作符，它的inputs只会有一个operator
 * @author nmg
 */
public class Project extends UnaryOperator {
	private List<Attribute> attributes;
	
	/**
	 * Create a new project operator.
	 * @param input Child operator
	 * @param attributes List of attributes to be projected
	 */
	public Project(Operator input, List<Attribute> attributes) {
		super(input);
		this.attributes = attributes;
	}

	/**
	 * Return the list of attributes projected by this operator
	 * @return List of attributes to be projected
	 */
	public List<Attribute> getAttributes() {
		return this.attributes;
	}
	
	/* (non-Javadoc)
	 * @see sjdb.UnaryOperator#accept(sjdb.OperatorVisitor)
	 */
	public void accept(PlanVisitor visitor) {
		// depth-first traversal - accept the
		// 通过递归，实现查询树后续遍历
		super.accept(visitor);
		// 处理当前节点
		visitor.visit(this);
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		String ret = "PROJECT [";
		Iterator<Attribute> iter = this.attributes.iterator();
		
		ret += iter.next().getName();
		
		while (iter.hasNext()) {
			ret += "," + iter.next().getName();
		}
		ret += "] (" + getInput().toString() + ")";
		
		return ret;
	}
}
