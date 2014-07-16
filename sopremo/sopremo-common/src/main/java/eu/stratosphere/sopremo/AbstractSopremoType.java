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
package eu.stratosphere.sopremo;

import java.io.IOException;
import java.util.Iterator;

/**
 * Provides basic implementations of the required methods of {@link ISopremoType}.
 */
public abstract class AbstractSopremoType implements ISopremoType {
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public AbstractSopremoType clone() {
		return SopremoEnvironment.getInstance().getEvaluationContext().getKryo(getClass().getClassLoader()).copy(this);
	}

	@SuppressWarnings("unchecked")
	public final <T extends AbstractSopremoType> T copy() {
		return (T) this.clone();
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	public AbstractSopremoType shallowClone() {
		return SopremoEnvironment.getInstance().getEvaluationContext().getKryo().copyShallow(this);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return toString(this);
	}

	protected void append(final Appendable appendable, final Iterable<? extends ISopremoType> children,
			final String separator)
			throws IOException {
		final Iterator<? extends ISopremoType> iterator = children.iterator();
		for (int index = 0; iterator.hasNext(); index++) {
			if (index > 0)
				appendable.append(separator);
			final ISopremoType child = iterator.next();
			if (child == null)
				appendable.append("!null!");
			else
				child.appendAsString(appendable);
		}
	}

	protected void checkCopyType(final AbstractSopremoType copy) {
		if (copy.getClass() != this.getClass())
			throw new AssertionError(String.format("Create copy returned wrong type. Expected %s but was %s",
				this.getClass(), copy.getClass()));
	}

	/**
	 * Returns a string representation of the given {@link ISopremoType}.
	 * 
	 * @param type
	 *        the SopremoType that should be used
	 * @return the string representation
	 */
	public static String toString(final ISopremoType type) {
		final StringBuilder builder = new StringBuilder();
		try {
			type.appendAsString(builder);
		} catch (final IOException e) {
			// cannot happen
		}
		return builder.toString();
	}
}
