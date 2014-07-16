package eu.stratosphere.sopremo.base;

import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.operator.ElementaryOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.Property;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.JsonUtil;
import eu.stratosphere.sopremo.type.NullNode;

/**
 * Splits an array into multiple tuples.<br>
 * This operator provides a means to emit more than one tuple in contrast to most other base operators.
 */
@InputCardinality(1)
@Name(verb = "split array")
public class ArraySplit extends ElementaryOperator<ArraySplit> {
	private EvaluationExpression arrayPath = EvaluationExpression.VALUE;

	{
		setResultProjection(new ArrayAccess(0));
	}

	public EvaluationExpression getArrayPath() {
		return this.arrayPath;
	};

	/**
	 * Sets the arrayPath to the specified value.
	 * 
	 * @param arrayPath
	 *        the arrayPath to set
	 */
	@Property
	@Name(preposition = "on")
	public void setArrayPath(final EvaluationExpression arrayPath) {
		if (arrayPath == null)
			throw new NullPointerException("arrayPath must not be null");

		this.arrayPath = arrayPath;
	}

	/**
	 * (element, index, array, node) -&gt; value
	 */
	public void setResultProjection(final ResultField... fields) {
		final int[] indices = new int[fields.length];
		for (int index = 0; index < indices.length; index++)
			indices[index] = fields[index].ordinal();
		this.setResultProjection(ArrayAccess.arrayWithIndices(indices));
	}

	public ArraySplit withResultProjection(final ResultField... fields) {
		this.setResultProjection(fields);
		return this;
	}

	public ArraySplit withArrayPath(final EvaluationExpression arrayPath) {
		this.arrayPath = arrayPath;
		return this;
	}

	public static class Implementation extends SopremoMap {
		private EvaluationExpression arrayPath;

		@Override
		protected void map(final IJsonNode value, final JsonCollector<IJsonNode> out) {
			final IJsonNode target = this.arrayPath.evaluate(value);
			if (!(target instanceof IArrayNode<?>))
				throw new EvaluationException("Cannot split non-array");
			final IArrayNode<?> array = (IArrayNode<?>) target;

			int index = 0;
			final IntNode indexNode = IntNode.valueOf(0);
			final IArrayNode<IJsonNode> contextNode = JsonUtil.asArray(NullNode.getInstance(), indexNode, array, value);
			for (final IJsonNode element : array) {
				contextNode.set(0, element);
				indexNode.setValue(index);
				out.collect(contextNode);
				index++;
			}
		}
	}

	public enum ResultField {
		Element, Index, Array, WholeValue;
	}
}
