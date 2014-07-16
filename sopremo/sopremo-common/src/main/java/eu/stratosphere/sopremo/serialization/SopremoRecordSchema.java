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
package eu.stratosphere.sopremo.serialization;

import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import eu.stratosphere.compiler.postpass.AbstractSchema;
import eu.stratosphere.compiler.postpass.ConflictingFieldTypeInfoException;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 */
public class SopremoRecordSchema extends AbstractSchema<Class<? extends IJsonNode>> {
	private final IntSet usedKeys = new IntAVLTreeSet();

	public void add(final int pos) {
		if (pos != SopremoRecordLayout.VALUE_INDEX)
			this.usedKeys.add(pos);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.compiler.postpass.AbstractSchema#addType(int, java.lang.Object)
	 */
	@Override
	public void addType(final int pos, final Class<? extends IJsonNode> type) throws ConflictingFieldTypeInfoException {
		if (pos != SopremoRecordLayout.VALUE_INDEX)
			this.usedKeys.add(pos);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.compiler.postpass.AbstractSchema#getType(int)
	 */
	@Override
	public Class<? extends IJsonNode> getType(final int field) {
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.usedKeys.hashCode();
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		SopremoRecordSchema other = (SopremoRecordSchema) obj;
		return this.usedKeys.equals(other.usedKeys);
	}

	/**
	 * Returns the usedKeys.
	 * 
	 * @return the usedKeys
	 */
	public IntSet getUsedKeys() {
		return this.usedKeys;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Iterable#iterator()
	 */
	@Override
	public Iterator<Entry<Integer, Class<? extends IJsonNode>>> iterator() {
		return new Iterator<Map.Entry<Integer, Class<? extends IJsonNode>>>() {
			Iterator<Integer> iterator = SopremoRecordSchema.this.usedKeys.iterator();

			/*
			 * (non-Javadoc)
			 * @see java.util.Iterator#hasNext()
			 */
			@Override
			public boolean hasNext() {
				return this.iterator.hasNext();
			}

			/*
			 * (non-Javadoc)
			 * @see java.util.Iterator#next()
			 */
			@Override
			public Entry<Integer, Class<? extends IJsonNode>> next() {
				return new AbstractMap.SimpleEntry<Integer, Class<? extends IJsonNode>>(this.iterator.next(), null);
			}

			@Override
			public void remove() {
				this.iterator.remove();
			}
		};
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return this.usedKeys.toString();
	}

}
