package eu.stratosphere.sopremo.type;

import java.io.IOException;

import com.esotericsoftware.kryo.DefaultSerializer;

import eu.stratosphere.sopremo.SingletonSerializer;
import eu.stratosphere.util.Immutable;

/**
 * This node represents a missing value.
 */
@Immutable
@DefaultSerializer(MissingNode.MissingSerializer.class)
public class MissingNode extends AbstractJsonNode implements IPrimitiveNode {

	private final static MissingNode Instance = new MissingNode();

	/**
	 * Initializes a MissingNode. This constructor is needed for serialization and
	 * deserialization of MissingNodes, please use MissingNode.getInstance() to get the instance of MissingNode.
	 */
	MissingNode() {
	}

	@Override
	public void appendAsString(final Appendable sb) throws IOException {
		sb.append("<missing>");
	}

	@Override
	public void clear() {
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.AbstractJsonNode#copy()
	 */
	@Override
	public MissingNode clone() {
		return this;
	}

	@Override
	public int compareToSameType(final IJsonNode other) {
		return 0;
	}

	@Override
	public void copyValueFrom(final IJsonNode otherNode) {
		this.checkForSameType(otherNode);
	}

	@Override
	public boolean equals(final Object o) {
		return this == o;
	}

	@Override
	public Class<MissingNode> getType() {
		return MissingNode.class;
	}

	@Override
	public int hashCode() {
		return 42;
	}

	/**
	 * Returns the instance of MissingNode.
	 * 
	 * @return the instance of MissingNode
	 */
	public static MissingNode getInstance() {
		return Instance;
	}

	public static class MissingSerializer extends SingletonSerializer {
		/**
		 * Initializes MissingNode.MissingSerializer.
		 */
		public MissingSerializer() {
			super(MissingNode.getInstance());
		}
	}
}
