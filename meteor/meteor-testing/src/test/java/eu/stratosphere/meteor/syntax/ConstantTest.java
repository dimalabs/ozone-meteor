/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.meteor.syntax;

import org.junit.Test;

import eu.stratosphere.meteor.MeteorParseTest;
import eu.stratosphere.sopremo.base.Selection;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.TernaryExpression;
import eu.stratosphere.sopremo.io.Sink;
import eu.stratosphere.sopremo.io.Source;
import eu.stratosphere.sopremo.operator.SopremoPlan;

/**
 * 
 */
public class ConstantTest extends MeteorParseTest {
	/**
	 * 
	 */
	@Test
	public void test() {
		final SopremoPlan actualPlan =
			this.parseScript("path = 'file://long/path/';" + 
				"$li = read from path + 'lineitem.json';\n" +
				"$li = filter $li where $li.l_linenumber >= 1;\n" +
				"write $li to 'file://q1.json';\n");

		final Source input = new Source("file://long/path/lineitem.json");
		final Selection filter = new Selection().
			withInputs(input).
			withCondition(new ComparativeExpression(new ObjectAccess("l_linenumber"),
				BinaryOperator.GREATER_EQUAL,
				new ConstantExpression(1)));

		final Sink sink = new Sink("file://q1.json").withInputs(filter);
		final SopremoPlan expectedPlan = new SopremoPlan();
		expectedPlan.setSinks(sink);

		assertPlanEquals(expectedPlan, actualPlan);
	} 
	
	/**
	 * 
	 */
	@Test
	public void testConstantInTernary() {
		final SopremoPlan actualPlan =
			this.parseScript("switchAtoB = 1; a = 'x'; b = 'y';" + 
				"$li = read from 'file://lineitem.json';\n" +
				"$li = filter $li where $li.l_itemname == (switchAtoB ? a : b);\n" +
				"write $li to 'file://q1.json';\n");

		final Source input = new Source("file://lineitem.json");
		final Selection filter = new Selection().
			withInputs(input).
			withCondition(new ComparativeExpression(new ObjectAccess("l_itemname"),
				BinaryOperator.EQUAL,
				new TernaryExpression(new ConstantExpression(1), new ConstantExpression('x'), new ConstantExpression('y'))));

		final Sink sink = new Sink("file://q1.json").withInputs(filter);
		final SopremoPlan expectedPlan = new SopremoPlan();
		expectedPlan.setSinks(sink);

		assertPlanEquals(expectedPlan, actualPlan);
	} 
}
