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
package eu.stratosphere.sopremo.function;

import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.SubArrayNode;

/**
 */
public abstract class SopremoVarargFunction1<Arg1 extends IJsonNode> extends SopremoVarargFunction {
	private transient final SubArrayNode<IJsonNode> varargArguments = new SubArrayNode<IJsonNode>();

	/**
	 * Initializes SopremoVarargFunction1.
	 */
	public SopremoVarargFunction1() {
		super(1);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.function.Callable#call(java.lang.Object)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public IJsonNode call(final IArrayNode<IJsonNode> params) {
		this.varargArguments.init(params, 1);
		return this.call((Arg1) params.get(0), this.varargArguments);
	}

	protected abstract IJsonNode call(Arg1 arg1, IArrayNode<IJsonNode> varargs);
}
