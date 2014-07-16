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
package eu.stratosphere.sopremo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Array;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;

import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;

import eu.stratosphere.util.KryoUtil;

/**
 * @param <T>
 */
@Ignore
public abstract class EqualVerifyTest<T> extends TestBase {
	protected T first, second;

	protected Collection<T> more;

	protected Class<T> type;

	/**
	 * Initializes EqualVerifyTest.
	 */
	public EqualVerifyTest() {
		super();
	}

	@SuppressWarnings("unchecked")
	@Before
	public void initInstances() {
		if (this.type == null) {
			final Type boundParamType =
				((ParameterizedType) TypeToken.of(this.getClass()).getSupertype(EqualVerifyTest.class).getType()).getActualTypeArguments()[0];
			this.type = (Class<T>) TypeToken.of(boundParamType).getRawType();
			this.createInstances();
		}
	}

	@SuppressWarnings("unchecked")
	public void shouldComplyEqualsOperator() {
		if (this.first == null)
			Assert.fail("Cannot create default instance; "
				+ "please override createDefaultInstance or shouldComplyEqualsOperator");
		try {
			// check if there is a equal method
			this.first.getClass().getDeclaredMethod("equals", Object.class);
			this.shouldComplyEqualsOperator(this.first, this.second,
				this.more.toArray((T[]) Array.newInstance(this.type, this.more.size())));
		} catch (final NoSuchMethodException e) {
			// then we do not have to test it
		}
	}

	public void shouldComplyEqualsOperator(final T first, final T second,
			@SuppressWarnings("unchecked") final T... more) {
		final EqualsVerifier<T> equalVerifier = EqualsVerifier.forExamples(first, second, more);
		this.initVerifier(equalVerifier);
		equalVerifier.verify();
	}

	@Test
	public void testKryoSerialization() {
		for (final Object original : Iterables.concat(Arrays.asList(this.first, this.second), this.more))
			this.testKryoSerialization(original);
	}

	protected abstract T createDefaultInstance(final int index);

	protected void createInstances() {
		this.initInstances(this.createDefaultInstance(0), this.createDefaultInstance(1),
			Collections.singleton(this.createDefaultInstance(2)));
	}

	protected void initInstances(final T first, final T second, final Collection<T> more) {
		this.first = first;
		this.second = second;
		this.more = more;
	}

	protected void initVerifier(final EqualsVerifier<T> equalVerifier) {
		final BitSet blackBitSet = new BitSet();
		blackBitSet.set(1);
		final ArrayList<Object> redList = new ArrayList<Object>();
		redList.add(null);
		final ArrayList<Object> blackList = new ArrayList<Object>(redList);
		blackList.add(null);
		final Map<Object, Object> blackMap = new HashMap<Object, Object>();
		blackMap.put("test", null);

		equalVerifier
			.suppress(Warning.NULL_FIELDS)
			.suppress(Warning.NONFINAL_FIELDS)
			.withPrefabValues(BitSet.class, new BitSet(), blackBitSet)
			.withPrefabValues(List.class, redList, blackList)
			.withPrefabValues(Map.class, new HashMap<Object, Object>(), blackMap)
			.usingGetClass();
	}

	protected void testKryoSerialization(final Object original) {
		final Kryo kryo = KryoUtil.getKryo();

		kryo.reset();
		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		final Output output = new Output(baos);
		kryo.writeClassAndObject(output, original);
		output.close();

		kryo.reset();
		final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		final Object deserialized = kryo.readClassAndObject(new Input(bais));

		Assert.assertEquals(original, deserialized);
	}

}