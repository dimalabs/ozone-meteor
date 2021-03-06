package eu.stratosphere.sopremo.expressions;

import static eu.stratosphere.sopremo.type.JsonUtil.createArrayNode;
import static eu.stratosphere.sopremo.type.JsonUtil.createObjectNode;
import static eu.stratosphere.sopremo.type.JsonUtil.createValueNode;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.MissingNode;

public class FieldAccessTest extends EvaluableExpressionTest<ObjectAccess> {
	@Test
	public void shouldAccessFieldOfSingleObject() {
		final IJsonNode result = new ObjectAccess("fieldName").evaluate(
			createObjectNode("fieldName", 42, "fieldName2", 12));
		Assert.assertEquals(createValueNode(42), result);
	}

	@Test
	public void shouldFailIfPrimitive() {
		final IJsonNode result = new ObjectAccess("fieldName").evaluate(createValueNode(42));
		Assert.assertSame(MissingNode.getInstance(), result);
	}

	@Test
	public void shouldReturnMissingNodeIfArray() {
		final IJsonNode result = new ObjectAccess("fieldName").evaluate(createArrayNode(1, 2, 3));
		Assert.assertSame(MissingNode.getInstance(), result);
	}

	@Override
	protected ObjectAccess createDefaultInstance(final int index) {
		return new ObjectAccess(String.valueOf(index));
	}

}
