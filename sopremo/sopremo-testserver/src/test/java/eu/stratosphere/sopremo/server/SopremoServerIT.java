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
package eu.stratosphere.sopremo.server;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;

import eu.stratosphere.sopremo.base.Selection;
import eu.stratosphere.sopremo.execution.ExecutionRequest;
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
import eu.stratosphere.sopremo.operator.ConfigurableSopremoType;
import eu.stratosphere.sopremo.operator.SopremoPlan;
import eu.stratosphere.sopremo.type.JsonUtil;

/**
 */
public class SopremoServerIT {
	private SopremoTestServer testServer;

	private File inputDir;
	private File outputDir;

	/**
	 * Initializes SopremoServerIT.
	 */
	public SopremoServerIT() {
	}

	@Before
	public void setup() throws Exception {
		this.testServer = new SopremoTestServer(false);
		this.inputDir = this.testServer.createTempDir();
		this.outputDir = this.testServer.createTempDir();

		this.testServer.createFile(this.inputDir.getName()+"/input1.json",
			JsonUtil.createObjectNode("name", "Jon Doe", "income", 20000, "mgr", false),
			JsonUtil.createObjectNode("name", "Vince Wayne", "income", 32500, "mgr", false));
		this.testServer.createFile(this.inputDir.getName()+"/input2.json",
			JsonUtil.createObjectNode("name", "Jane Dean", "income", 72000, "mgr", true),
			JsonUtil.createObjectNode("name", "Alex Smith", "income", 25000, "mgr", false));
	}

	@After
	public void teardown() throws Exception {
		this.testServer.close();
	}

	@Test
	public void testFailIfInvalidPlan() throws IOException, InterruptedException {
		final SopremoPlan plan = new SopremoPlan();
		plan.setSinks(new Sink("file:///invalidSink"));

		ExecutionResponse response = this.testServer.execute(new ExecutionRequest(plan));
		response = this.waitForStateToFinish(response, ExecutionState.ENQUEUED);

		Assert.assertSame(ExecutionState.ERROR, response.getState());
		Assert.assertNotSame("", response.getDetails());
	}

	@Test
	public void testFailIfRuntimeException() throws IOException, InterruptedException {
		final SopremoPlan plan = this.createPlan("output.json");
		for (final ConfigurableSopremoType op : plan.getContainedOperators())
			if (op instanceof Selection)
				((Selection) op).setCondition(new UnaryExpression(new UnevaluableExpression("test failure")));

		ExecutionResponse response = this.testServer.execute(new ExecutionRequest(plan));
		response = this.waitForStateToFinish(response, ExecutionState.ENQUEUED);
		response = this.waitForStateToFinish(response, ExecutionState.RUNNING);

		Assert.assertSame(ExecutionState.ERROR, response.getState());
		Assert.assertNotSame("", response.getDetails());
	}

	@Test
	public void testFailIfSubmissionFails() throws IOException, InterruptedException {
		// job manager cannot determine input splits
		this.testServer.delete(this.inputDir.getName(), true);
		final SopremoPlan plan = this.createPlan("output.json");

		ExecutionResponse response = this.testServer.execute(new ExecutionRequest(plan));
		response = this.waitForStateToFinish(response, ExecutionState.ENQUEUED);
		response = this.waitForStateToFinish(response, ExecutionState.RUNNING);

		Assert.assertSame(ExecutionState.ERROR, response.getState());
		Assert.assertNotSame("", response.getDetails());
	}

	@Test
	public void testMultipleSuccessfulExecutions() throws IOException, InterruptedException {
		final ExecutionResponse[] responses = new ExecutionResponse[3];
		for (int index = 0; index < responses.length; index++) {
			final SopremoPlan plan = this.createPlan("output" + index + ".json");
			responses[index] = this.testServer.execute(new ExecutionRequest(plan));
		}

		for (int index = 0; index < responses.length; index++) {
			responses[index] = this.waitForStateToFinish(responses[index], ExecutionState.ENQUEUED);
			responses[index] = this.waitForStateToFinish(responses[index], ExecutionState.RUNNING);

			Assert.assertSame(ExecutionState.FINISHED, responses[index].getState());
			Assert.assertSame("", responses[index].getDetails());

			this.testServer.checkContentsOf(this.outputDir.getName()+"/"+"output" + index + ".json",
				JsonUtil.createObjectNode("name", "Vince Wayne", "income", 32500, "mgr", false),
				JsonUtil.createObjectNode("name", "Jane Dean", "income", 72000, "mgr", true));
		}
	}

	@Test
	public void testSuccessfulExecution() throws IOException, InterruptedException {
		final SopremoPlan plan = this.createPlan("output.json");

		ExecutionResponse response = this.testServer.execute(new ExecutionRequest(plan));
		response = this.waitForStateToFinish(response, ExecutionState.ENQUEUED);
		response = this.waitForStateToFinish(response, ExecutionState.RUNNING);

		Assert.assertSame(ExecutionState.FINISHED, response.getState());
		Assert.assertSame("", response.getDetails());

		this.testServer.checkContentsOf(this.outputDir.getName()+"/"+"output.json",
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
		final Sink output = new Sink(this.testServer.createFile(this.outputDir.getName()+"/"+outputName).toURI().toString()).withInputs(selection);
		plan.setSinks(output);
		return plan;
	}

	private ExecutionResponse waitForStateToFinish(final ExecutionResponse response, final ExecutionState status)
			throws IOException, InterruptedException {
		return SopremoTestServer.waitForStateToFinish(this.testServer, response, status);
	}

}
