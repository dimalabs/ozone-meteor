package eu.stratosphere.sopremo.base;

import java.util.Map.Entry;

import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.operator.ElementaryOperator;
import eu.stratosphere.sopremo.operator.Internal;
import eu.stratosphere.sopremo.operator.Property;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IObjectNode;
import eu.stratosphere.sopremo.type.JsonUtil;
import eu.stratosphere.sopremo.type.NullNode;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * Splits an object into multiple outgoing tuples.<br>
 * This operator provides a means to emit more than one tuple in contrast to most other base operators.
 */
// TODO: needs to be refactored
@Internal
public class ObjectSplit extends ElementaryOperator<ObjectSplit> {
	private EvaluationExpression objectPath = EvaluationExpression.VALUE,
			valueProjection = new ArrayAccess(0);

	public EvaluationExpression getObjectPath() {
		return this.objectPath;
	}

	public EvaluationExpression getValueProjection() {
		return this.valueProjection;
	}

	@Property
	public ObjectSplit setObjectProjection(final EvaluationExpression objectPath) {
		this.objectPath = objectPath;
		return this;
	}

	/**
	 * (element, index/fieldName, array/object, node) -&gt; value
	 * 
	 * @param valueProjection
	 */
	@Property
	public void setValueProjection(final EvaluationExpression valueProjection) {
		this.valueProjection = valueProjection;
	}

	public ObjectSplit withObjectProjection(final EvaluationExpression objectProjection) {
		this.setObjectProjection(objectProjection);
		return this;
	}

	/**
	 * (element, index/fieldName, array/object, node) -&gt; value
	 * 
	 * @param valueProjection
	 * @return this
	 */
	public ObjectSplit withValueProjection(final EvaluationExpression valueProjection) {
		this.setValueProjection(valueProjection);
		return this;
	}

	public static class Implementation extends SopremoMap {
		private EvaluationExpression objectPath;

		private EvaluationExpression valueProjection;

		@Override
		protected void map(final IJsonNode value, final JsonCollector<IJsonNode> out) {
			final IJsonNode targetValue = this.objectPath.evaluate(value);
			if (!(targetValue instanceof IObjectNode))
				throw new EvaluationException("Cannot split non-object");
			final IObjectNode object = (IObjectNode) targetValue;

			final TextNode fieldNode = TextNode.valueOf("");
			final IArrayNode<IJsonNode> contextNode =
				JsonUtil.asArray(NullNode.getInstance(), fieldNode, object, value);
			for (final Entry<String, IJsonNode> entry : object) {
				fieldNode.setValue(entry.getKey());
				out.collect(this.valueProjection.evaluate(contextNode));
			}
		}
	}
}
