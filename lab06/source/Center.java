package k_means;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.WritableComparable;

public class Center implements WritableComparable<Center> {
	public float[] coor;
	public int dimensions;
	
	public Center() {
		dimensions = 0;
		coor = new float[dimensions];
	}
	
	public Center(int dim) {
		dimensions = dim;
		coor = new float[dimensions];
	}
	
	public Center(int dim, float[] data) {
		dimensions = dim;
		coor = new float[dim];
		for(int i = 0; i < dim; ++i)
			coor[i] = data[i];
	}
	
	public Center(float[] data) {
		dimensions = data.length;
		coor = new float[dimensions];
		for(int i = 0; i < dimensions; ++i)
			coor[i] = data[i];
	}
	
	public Center(int dim, Integer[] data) {
		dimensions = dim;
		coor = new float[dim];
		for(int i = 0; i < dim; ++i) 
			coor[i] = data[i];
	}
	
	
	public float distance(Center other) {
		float d = 0;
		for(int i = 0; i < dimensions; ++i) {
			float x = coor[i] - other.coor[i];
			d += x * x;
		}
		return (float)Math.sqrt(d);
	}
	
	public void set(float[] coordinate) {
		dimensions = coor.length;
		this.coor = coordinate;
	}
	
	/**
	 * Read from context (Use to recover the class in Reducer)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		ArrayList<Float> list = new ArrayList<Float>();
		try {
		while(true) {
			float f = in.readFloat();
			list.add(f);
		}
		}
		catch(IOException e) {
			//ignore
		}
		dimensions = list.size();
		coor = new float[dimensions];
		for(int i = 0; i < dimensions; ++i)
			coor[i] = list.get(i);
	}

	/**
	 * Write to context (used in Mapper)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		for(float f : coor)
			out.writeFloat(f);
	}

	/**
	 * Also used to check equality between Mapper and Reducer
	 */
	@Override
	public int compareTo(Center o) {
		return toString().compareTo(o.toString());
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < dimensions; ++i) {
			sb.append("" + coor[i] + " ");
		}
		
		return sb.toString();
	}
	
}
