/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.sopremo.client;

import java.io.File;
import java.io.IOException;
import java.util.Deque;
import java.util.LinkedList;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.sopremo.base.Selection;
import eu.stratosphere.sopremo.execution.ExecutionRequest.ExecutionMode;
import eu.stratosphere.sopremo.execution.ExecutionResponse;
import eu.stratosphere.sopremo.execution.ExecutionResponse.ExecutionState;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.OrExpression;
import eu.stratosphere.sopremo.expressions.UnaryExpression;
import eu.stratosphere.sopremo.expressions.UnevaluableExpression;
import eu.stratosphere.sopremo.io.Sink;
import eu.stratosphere.sopremo.io.Source;
import eu.stratosphere.sopremo.operator.Operator;
import eu.stratosphere.sopremo.operator.SopremoPlan;
import eu.stratosphere.sopremo.server.SopremoTestServer;
import eu.stratosphere.sopremo.type.JsonUtil;

/**
 */
public class DefaultClientIT {

	private SopremoTestServer testServer;

	private DefaultClient client;

	private StateRecorder stateRecorder;

	private File inputDir;
	private File outputDir;

	/**
	 * Initializes DefaultClientIT.
	 */
	public DefaultClientIT() {
	}

	@Before
	public void setup() throws Exception {
		this.testServer = new SopremoTestServer(true);
		this.inputDir = this.testServer.createTempDir();
		this.outputDir = this.testServer.createTempDir();

		this.testServer.createFile(this.inputDir.getName()+"/input1.json",
			JsonUtil.createObjectNode("name", "Jon Doe", "income", 20000, "mgr", false),
			JsonUtil.createObjectNode("name", "Vince Wayne", "income", 32500, "mgr", false));
		this.testServer.createFile(this.inputDir.getName()+"/input2.json",
			JsonUtil.createObjectNode("name", "Jane Dean", "income", 72000, "mgr", true),
			JsonUtil.createObjectNode("name", "Alex Smith", "income", 25000, "mgr", false));

		this.client = new DefaultClient();
		this.client.setServerAddress(this.testServer.getServerAddress());
		this.client.setUpdateTime(100);

		this.stateRecorder = new StateRecorder();
	}

	@After
	public void teardown() throws Exception {
		this.client.close();
		this.testServer.close();
	}

	@Test
	public void testFailIfInvalidPlan() {
		final SopremoPlan plan = new SopremoPlan();
		plan.setSinks(new Sink("file:///invalidSink"));

		this.client.submit(plan, this.stateRecorder);
		Assert.assertSame(ExecutionState.SETUP, this.stateRecorder.getStates().getFirst());
		Assert.assertSame(ExecutionState.ERROR, this.stateRecorder.getStates().getLast());
		Assert.assertNotSame("", this.stateRecorder.getLastDetail());
	}

	@Test
	public void testFailIfRuntimeException() throws IOException {
		final SopremoPlan plan = this.createPlan("output.json");
		for (final Operator<?> op : plan.getContainedOperators())
			if (op instanceof Selection)
				((Selection) op).setCondition(new UnaryExpression(new UnevaluableExpression("test failure")));

		this.client.submit(plan, this.stateRecorder);
		Assert.assertSame(ExecutionState.SETUP, this.stateRecorder.getStates().getFirst());
		Assert.assertSame(ExecutionState.ERROR, this.stateRecorder.getStates().getLast());
		Assert.assertNotSame("", this.stateRecorder.getLastDetail());
	}

	@Test
	public void testFailIfSubmissionFails() throws IOException {
		// job manager cannot determine input splits
		this.testServer.delete(this.inputDir.getName(), true);
		final SopremoPlan plan = this.createPlan("output.json");

		this.client.submit(plan, this.stateRecorder);
		Assert.assertSame(ExecutionState.SETUP, this.stateRecorder.getStates().getFirst());
		Assert.assertSame(ExecutionState.ERROR, this.stateRecorder.getStates().getLast());
		Assert.assertNotSame("", this.stateRecorder.getLastDetail());
	}

	@Test
	public void testMultipleSuccessfulExecutions() throws IOException {
		final ExecutionResponse[] responses = new ExecutionResponse[3];
		for (int index = 0; index < responses.length; index++) {
			final SopremoPlan plan = this.createPlan("output" + index + ".json");
			this.client.submit(plan, this.stateRecorder);
			Assert.assertSame(ExecutionState.SETUP, this.stateRecorder.getStates().getFirst());
			Assert.assertSame(ExecutionState.FINISHED, this.stateRecorder.getStates().getLast());

			this.testServer.checkContentsOf(this.outputDir.getName()+"/" +"output" + index + ".json",
				JsonUtil.createObjectNode("name", "Vince Wayne", "income", 32500, "mgr", false),
				JsonUtil.createObjectNode("name", "Jane Dean", "income", 72000, "mgr", true));
		}
	}

	@Test
	public void testSuccessfulExecution() throws IOException {
		final SopremoPlan plan = this.createPlan("output.json");

		this.client.submit(plan, this.stateRecorder);
		Assert.assertSame(ExecutionState.SETUP, this.stateRecorder.getStates().getFirst());
		Assert.assertSame(ExecutionState.FINISHED, this.stateRecorder.getStates().getLast());
		Assert.assertEquals("", this.stateRecorder.getLastDetail());

		this.testServer.checkContentsOf(this.outputDir.getName()+"/" +"output.json",
			JsonUtil.createObjectNode("name", "Vince Wayne", "income", 32500, "mgr", false),
			JsonUtil.createObjectNode("name", "Jane Dean", "income", 72000, "mgr", true));
	}

	@Test
	public void testSuccessfulExecutionWithStatistics() throws IOException {
		final SopremoPlan plan = this.createPlan("output.json");

		this.client.setExecutionMode(ExecutionMode.RUN_WITH_STATISTICS);
		this.client.submit(plan, this.stateRecorder);
		Assert.assertSame(ExecutionState.SETUP, this.stateRecorder.getStates().getFirst());
		Assert.assertSame(ExecutionState.FINISHED, this.stateRecorder.getStates().getLast());
		Assert.assertFalse("".equals(this.stateRecorder.getLastDetail()));

		this.testServer.checkContentsOf(this.outputDir.getName()+"/" +"output.json",
			JsonUtil.createObjectNode("name", "Vince Wayne", "income", 32500, "mgr", false),
			JsonUtil.createObjectNode("name", "Jane Dean", "income", 72000, "mgr", true));
	}

	private SopremoPlan createPlan(final String outputName) throws IOException {
		final SopremoPlan plan = new SopremoPlan();
		final Source input = new Source(this.inputDir.toURI().toString());
		final Selection selection = new Selection().
			withCondition(
				new OrExpression(
					new UnaryExpression(JsonUtil.createPath("0", "mgr")),
					new ComparativeExpression(JsonUtil.createPath("0", "income"), BinaryOperator.GREATER,
						new ConstantExpression(30000)))).
			withInputs(input);
		final Sink output = new Sink(this.testServer.createFile(this.outputDir.getName()+"/" +outputName).toURI().toString()).withInputs(selection);
		plan.setSinks(output);
		return plan;
	}

	private static final class StateRecorder extends StateListener {
		private final Deque<ExecutionState> states = new LinkedList<ExecutionState>();

		private String lastDetail;

		/**
		 * Returns the lastDetail.
		 * 
		 * @return the lastDetail
		 */
		public String getLastDetail() {
			return this.lastDetail;
		}

		/**
		 * Returns the states.
		 * 
		 * @return the states
		 */
		public Deque<ExecutionState> getStates() {
			return this.states;
		}

		@Override
		public void stateChanged(final ExecutionState executionStatus, final String detail) {
			this.states.add(executionStatus);
			this.lastDetail = detail;
			System.out.println(detail);
		}
	}

}
