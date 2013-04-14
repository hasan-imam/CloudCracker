import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class Serializer {
	/**
	 * Converts object to a byte array
	 * @param obj Object to be converted into byte array. Must be serializable
	 * @return Resultant byte array
	 */
    public static byte[] serialize(Object obj) throws IOException {
    	if (obj == null) {
			return null;
		}
    	assert(obj instanceof Serializable);
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        ObjectOutputStream o = new ObjectOutputStream(b);
        o.writeObject(obj);
        return b.toByteArray();
    }
    
    /**
     * Reconstructs Object from byte array
     * @param bytes Byte array that needs to be converted to an object
     * @return Resultant object
     */
    public static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
    	if (bytes == null) {
			return null;
		}
        ByteArrayInputStream b = new ByteArrayInputStream(bytes);
        ObjectInputStream o = new ObjectInputStream(b);
        return o.readObject();
    }
}

