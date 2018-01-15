package pagerank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;

public class SortPair implements WritableComparable{
	private Text title;
	private double rank;

	public SortPair() {
		title = new Text();
		rank = 0.0;
	}

	public SortPair(Text title, double rank) {
		//TODO: constructor
		this.title = title;
		this.rank = rank;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		title.write(out);
		out.writeDouble(rank);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		title.readFields(in);
		rank = in.readDouble();
	}

	public Text getTitle() {
		return title;
	}

	public double getRank() {
		return rank;
	}

	@Override
	public int compareTo(Object o) {

		double thisRank = this.getRank();
		double thatRank = ((SortPair)o).getRank();

		Text thisTitle = this.getTitle();
		Text thatTitle = ((SortPair)o).getTitle();

		// Compare between two objects
		// First order by average, and then sort them lexicographically in ascending order
		if(thisRank== thatRank){
			return thisTitle.compareTo(thatTitle);
		}
		else{
			if(thisRank > thatRank)
				return -1;
			else
				return 1;	
		}
	}
} 
