package eu.stratosphere.sopremo.operator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import eu.stratosphere.api.common.functions.AbstractFunction;
import eu.stratosphere.api.common.functions.Function;
import eu.stratosphere.api.common.operators.SingleInputOperator;
import eu.stratosphere.api.common.operators.base.MapOperatorBase;
import eu.stratosphere.api.common.operators.util.UserCodeClassWrapper;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.pact.SopremoReduce;
import eu.stratosphere.sopremo.pact.SopremoReduceOperator;
import eu.stratosphere.sopremo.serialization.SopremoRecordLayout;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IStreamNode;

/**
 * The class <code>ElementaryOperatorTest</code> contains tests for the class <code>{@link ElementaryOperator}</code>.
 */
public class ElementaryOperatorTest {
	/**
	 * 
	 */
	private static final SopremoRecordLayout LAYOUT = SopremoRecordLayout.create();

	@SuppressWarnings({ "rawtypes" })
	public ElementaryOperator<?> getDefault() {
		return new ElementaryOperator() {
		};
	}

	@Test
	public void getFunctionClassShouldReturnNullIfNoFunction() {
		assertEquals(null, new OperatorWithNoFunctions().getFunctionClass());
	}

	@Test
	public void getFunctionClassShouldReturnTheFirstFunction() {
		final Class<? extends Function> stubClass = new OperatorWithTwoFunctions().getFunctionClass();
		assertEquals(OperatorWithTwoFunctions.class, stubClass.getDeclaringClass());
		assertTrue(OperatorWithTwoFunctions.Implementation1.class == stubClass ||
			OperatorWithTwoFunctions.Implementation2.class == stubClass);
	}

	@Test
	public void getFunctionClassShouldReturnTheOnlyFunction() {
		assertEquals(OperatorWithOneFunction.Implementation.class,
			new OperatorWithOneFunction().getFunctionClass());
	}

	@Test(expected = IllegalStateException.class)
	public void getOperatorShouldFailIfNoFunction() {
		new OperatorWithNoFunctions().getOperator(LAYOUT);
	}

	@Test(expected = IllegalStateException.class)
	public void getOperatorShouldFailIfOnlyInstanceFunction() {
		new OperatorWithInstanceFunction().getOperator(LAYOUT);
	}

	@Test(expected = IllegalStateException.class)
	public void getOperatorShouldFailIfOnlyUnknownFunction() {
		new OperatorWithUnknownFunction().getOperator(LAYOUT);
	}

	@Test(expected = IllegalStateException.class)
	public void getOperatorShouldFailIfOperatorNotInstancable() {
		new OperatorWithUninstantiableFunction().getOperator(LAYOUT);
	}

	@Test
	public void getOperatorShouldReturnTheJoiningOperatorToTheFirstFunction() {
		final SopremoRecordLayout layout = SopremoRecordLayout.create(new ObjectAccess("someField"));
		final eu.stratosphere.api.common.operators.Operator contract =
			new OperatorWithTwoFunctions().getOperator(layout);
		assertEquals(SopremoReduceOperator.class, contract.getClass());
		Class<?> userCodeClass = contract.getUserCodeWrapper().getUserCodeClass();
		assertTrue(OperatorWithTwoFunctions.Implementation1.class == userCodeClass ||
				OperatorWithTwoFunctions.Implementation2.class == userCodeClass);
	}

	@Test
	public void getOperatorShouldReturnTheJoiningOperatorToTheOnlyFunction() {
		final eu.stratosphere.api.common.operators.Operator contract =
			new OperatorWithOneFunction().getOperator(LAYOUT);
		assertEquals(MapOperatorBase.class, contract.getClass());
		assertEquals(OperatorWithOneFunction.Implementation.class, contract.getUserCodeWrapper().getUserCodeClass());
	}

	@InputCardinality(1)
	static class OperatorWithInstanceFunction extends ElementaryOperator<OperatorWithInstanceFunction> {
		class Implementation extends SopremoMap {
			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.pact.SopremoMap#map(eu.stratosphere.sopremo.type.IJsonNode,
			 * eu.stratosphere.sopremo.pact.JsonCollector)
			 */
			@Override
			protected void map(final IJsonNode value, final JsonCollector<IJsonNode> out) {
			}
		}
	}

	@InputCardinality(1)
	static class OperatorWithNoFunctions extends ElementaryOperator<OperatorWithNoFunctions> {
	}

	@InputCardinality(1)
	static class OperatorWithOneFunction extends ElementaryOperator<OperatorWithOneFunction> {
		static class Implementation extends SopremoMap {
			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.pact.SopremoMap#map(eu.stratosphere.sopremo.type.IJsonNode,
			 * eu.stratosphere.sopremo.pact.JsonCollector)
			 */
			@Override
			protected void map(final IJsonNode value, final JsonCollector<IJsonNode> out) {
			}
		}
	}

	@InputCardinality(1)
	static class OperatorWithTwoFunctions extends ElementaryOperator<OperatorWithTwoFunctions> {
		/**
		 * Initializes ElementaryOperatorTest.OperatorWithTwoFunctions.
		 */
		public OperatorWithTwoFunctions() {
			this.setKeyExpressions(0, new ObjectAccess("someField"));
		}

		static class Implementation1 extends SopremoReduce {

			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.pact.SopremoReduce#reduce(eu.stratosphere.sopremo.type.ArrayNode,
			 * eu.stratosphere.sopremo.pact.JsonCollector)
			 */
			@Override
			protected void reduce(final IStreamNode<IJsonNode> values, final JsonCollector<IJsonNode> out) {
			}
		}

		static class Implementation2 extends SopremoReduce {
			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.pact.SopremoReduce#reduce(eu.stratosphere.sopremo.type.ArrayNode,
			 * eu.stratosphere.sopremo.pact.JsonCollector)
			 */
			@Override
			protected void reduce(final IStreamNode<IJsonNode> values, final JsonCollector<IJsonNode> out) {
			}
		}
	}

	@InputCardinality(1)
	static class OperatorWithUninstantiableFunction extends ElementaryOperator<OperatorWithUnknownFunction> {
		@InputCardinality(1)
		static class UninstanceableOperator extends SingleInputOperator<Function> {

			public UninstanceableOperator(final Class<? extends Function> clazz, final String name) {
				super(new UserCodeClassWrapper<Function>(clazz), name);
				throw new IllegalStateException("not instanceable");
			}

		}
	}

	@InputCardinality(1)
	static class OperatorWithUnknownFunction extends ElementaryOperator<OperatorWithUnknownFunction> {
		static class Implementation extends AbstractFunction {

			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.api.record.functions.Function#close()
			 */
			@Override
			public void close() throws Exception {
			}

			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.api.record.functions.Function#open(eu.stratosphere.configuration.Configuration)
			 */
			@Override
			public void open(final Configuration parameters) throws Exception {
			}
		}
	}

}
