import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;

public class My_DB {
	private HashMap<String, Integer> db = new HashMap<String, Integer>();  // will store all committed data
	private HashMap<Integer, Integer> counts = new HashMap<Integer, Integer>(); // will store record of how many times each value is present
	private Scanner in;

	public static void main(String args[]) {
		My_DB database = new My_DB();
		database.in = new Scanner(System.in);

		while (true) {
			String line = database.in.nextLine();
			boolean success = false;

			if (line.contains("UNSET")) {
				Scanner current = new Scanner(line);
				String name = null;
				if (current.hasNext()) {
					current.next();
					if (current.hasNext()) name = current.next();
					if (String.class.isInstance(name)) success = database.unset(name);
				}
				if (success == false) System.out.println("INVALID INPUT");
				current.close();

			} else if (line.contains("SET")) {
				Scanner current = new Scanner(line);
				String name = null;

				if (current.hasNext()) {
					current.next();
					if (current.hasNext()) name = current.next();
					if (current.hasNextInt() && String.class.isInstance(name)) success = database.set(name, current.nextInt());
				}
				if (success == false) System.out.println("INVALID INPUT");
				current.close();

			} else if (line.contains("GET")) {
				Scanner current = new Scanner(line);
				String name = null;
				if (current.hasNext()) {
					current.next();
					if (current.hasNext()) name = current.next();
					if (String.class.isInstance(name)) success = database.get(name);
				}
				if (success == false) System.out.println("INVALID INPUT");
				current.close();

			} else if (line.contains("NUMEQUALTO")) {
				Scanner current = new Scanner(line);
				Integer number = null;
				if (current.hasNext()) {
					current.next();
					if (current.hasNextInt()) {
						number = current.nextInt();
						success = database.numequalto(number);
					}
				}
				if (success == false) System.out.println("INVALID INPUT");
				current.close();

			} else if (line.contains("END")) {
				database.end();

			} else if (line.contains("BEGIN")) {
				HashMap<String, Integer> block = new HashMap<String, Integer>();  // initializing blank HashMap that will store new/updated values within block
				HashSet<String> unsets = new HashSet<String>();  // initializing a blank HashSet that will keep track of UNSET commands within block
				database.begin(block, unsets);

			} else if (line.contains("ROLLBACK") || line.contains("COMMIT")) {
				// ROLLBACK and COMMIT have no effect when called here, outside of a transaction block
				System.out.println("NO TRANSACTION");

			} else {
				System.out.println("INVALID INPUT");

			}

		}

	}

	public boolean set(String name, Integer value) {
		this.db.put(name, value);
		this.putOrIncrement(value, this.counts);
		return true;
	}

	public boolean get(String name) {
		System.out.println(this.db.get(name));
		return true;
	}

	public boolean unset(String name) {
		Integer number = this.db.get(name);
		this.db.remove(name);
		this.removeOrDecrement(number, this.counts);
		return true;

	}

	public boolean numequalto(Integer value) {
		Integer answer = this.counts.get(value);
		if (answer == null) {
			System.out.println(0);
		} else {
			System.out.println(answer);
		}
		return true;
	}

	public void end() {
		this.in.close();
		System.exit(0);
	}

	// used to keep track of the count of a given value
	// method used inside and outside of command blocks
	public void removeOrDecrement(Integer number, HashMap<Integer, Integer> count) {
		if (count.get(number) <=1 ) {
			count.remove(number);
		} else {
			Integer currentValue = count.get(number);
			count.put(number, currentValue-1);
		}
	}

	// used to keep track of the count of a given value
	// method used inside and outside of command blocks
	public void putOrIncrement(Integer number, HashMap<Integer, Integer> count) {
		if (!count.containsKey(number)) {
			count.put(number, 1);
		} else {
			Integer currentValue = count.get(number);
			count.put(number,  currentValue+1);
		}
	}

	public boolean begin(HashMap<String, Integer> parent, HashSet<String> parentUnsets) {
		boolean exit = false;
		HashMap<String, Integer> block = new HashMap<String, Integer>(parent);
		HashSet<String> blockUnsets = new HashSet<String>(parentUnsets);

		while (exit == false) {
			String line = this.in.nextLine();
			boolean success = false;

			if (line.contains("UNSET")) {
				Scanner current = new Scanner(line);
				String name = null;
				if (current.hasNext()) {
					current.next();
					if (current.hasNext()) name = current.next();
					if (String.class.isInstance(name)) success = this.unset(name, block, blockUnsets);
				}
				if (success == false) System.out.println("INVALID INPUT");
				current.close();

			} else if (line.contains("SET")) {
				Scanner current = new Scanner(line);
				String name = null;

				if (current.hasNext()) {
					current.next();
					if (current.hasNext()) name = current.next();
					if (current.hasNextInt() && String.class.isInstance(name)) success = this.set(name, current.nextInt(), block, blockUnsets);
				}
				if (success == false) System.out.println("INVALID INPUT");
				current.close();

			} else if (line.contains("GET")) {
				Scanner current = new Scanner(line);
				String name = null;
				if (current.hasNext()) {
					current.next();
					if (current.hasNext()) name = current.next();
					if (String.class.isInstance(name)) success = this.get(name, block, blockUnsets);
				}
				if (success == false) System.out.println("INVALID INPUT");
				current.close();

			} else if (line.contains("NUMEQUALTO")) {


			} else if (line.contains("END")) {
				this.end();

			} else if (line.contains("BEGIN")) {
				exit = this.begin(block, blockUnsets);

			} else if (line.contains("COMMIT")) {
				exit = this.commit(block, blockUnsets);

			} else if (line.contains("ROLLBACK")) {
				// to ROLLBACK, we simply return false, negating any changes in this block but allowing parent blocks to continue
				return false;

			} else {
				System.out.println("INVALID INPUT");
			}
		}

		// returning true will cause any begin() methods to exit
		return true;

	}



	// below are methods that will be used from within transaction blocks
	public boolean set(String name, Integer value, HashMap<String, Integer> block, HashSet<String> blockUnsets) {
		block.put(name,  value);
		if (blockUnsets.contains(name)) blockUnsets.remove(name); // this undoes a previous UNSET of this name from within an uncommitted block
		return true;
	}

	public boolean get(String name, HashMap<String, Integer> block, HashSet<String> blockUnsets) {
		if (block.containsKey(name)) {
			System.out.println(block.get(name));
		} else if (blockUnsets.contains(name)) {
			System.out.println("null");
		} else {
			System.out.println(db.get(name));
		}
		return true;
	}

	public boolean unset(String name, HashMap<String, Integer> block, HashSet<String> blockUnsets) {
		block.remove(name);
		blockUnsets.add(name); // this allows us to keep track of this UNSET before it is committed
		return true;
	}

	public void numequalto(Integer value, HashMap<String, Integer> block) {

	}

	public boolean commit(HashMap<String, Integer> block, HashSet<String> blockUnsets) {
		// to COMMIT this block's data, we SET all the items in block to db and UNSET all the names in blockUnsets
		Iterator<Entry<String, Integer>> updateSets = block.entrySet().iterator();
		Iterator<String> updateUnsets = blockUnsets.iterator();

		while (updateSets.hasNext()) {
			Map.Entry<String, Integer> current = (Map.Entry<String, Integer>)updateSets.next();
			this.set(current.getKey(), current.getValue());
		}

		while (updateUnsets.hasNext()) {
			this.unset(updateUnsets.next());
		}

		return true;
	}

}
