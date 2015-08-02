package k_means;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class Vector implements Writable {
	public int dimensions;
	public int[] coor;
	
	public Vector() {
		dimensions = 0;
		coor = new int[dimensions];
	}
	
	public Vector(int dim) {
		dimensions = dim;
		coor = new int[dim];
	}
	
	public Vector(int dim, int[] coordinate) {
		dimensions = dim;
		coor = coordinate;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		for(int i = 0; i < dimensions; ++i) {
			coor[i] = in.readInt();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		for(int i = 0; i < dimensions; ++i) {
			out.writeInt(coor[i]);
		}
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < dimensions; ++i) {
			sb.append("" + coor[i] + " ");
		}
		
		return sb.toString();
	}
	
	static Vector fromList(List<Integer> list) {
		int dim = list.size();
		Vector result = new Vector(dim);
		for(int i = 0; i < dim; ++i) {
			result.coor[i] = list.get(i);
		}
		return result;
	}
}
