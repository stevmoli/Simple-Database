import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;

public class My_DB {
	private HashMap<String, Integer> db = new HashMap<String, Integer>();  // will store all committed data
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


			} else if (line.contains("END")) {
				database.end();

			} else if (line.contains("BEGIN")) {
				HashSet<String> unsets = new HashSet<String>();  // this initializes a blank HashSet that will keep track of UNSET commands within the block
				database.begin(database.db, unsets);

			} else if (line.contains("ROLLBACK") || line.contains("COMMIT")) {
				// ROLLBACK and COMMIT have no effect when called here, outside of a transaction block
				System.out.println("NO TRANSACTION");

			} else {
				System.out.println("INVALID INPUT");

			}

		}

		// if rollback or commit commands are used here, print NO TRANSACTION
	}

	public boolean set(String name, Integer value) {
		this.db.put(name, value);
		return true;
	}

	public boolean get(String name) {
		System.out.println(this.db.get(name));
		return true;
	}

	public boolean unset(String name) {
		this.db.remove(name);
		return true;

	}

	public void numequalto(Integer value) {

	}

	public void end() {
		this.in.close();
		System.exit(0);
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
//				Scanner current = new Scanner(line);
//				current.next();
//				String name = current.next();
//				database.get(name);
//				current.close();

			} else if (line.contains("NUMEQUALTO")) {


			} else if (line.contains("END")) {
				this.end();

			} else if (line.contains("BEGIN")) {
				exit = this.begin(block, blockUnsets);

			} else if (line.contains("COMMIT")) {
				exit = this.commit(block);

			} else if (line.contains("ROLLBACK")) {
				// to ROLLBACK, we simply exit this method without committing
				exit = true;

			} else {
				System.out.println("INVALID INPUT");
			}
		}

		// returning true will cause any begin() methods to exit
		return true;

	}

//	public boolean beginNested(HashMap<String, Integer> parentBlock) {
//		boolean exit = false;
//		HashMap<String, Integer> block = new HashMap<String, Integer>(parentBlock);
//
//		while (exit == false) {
//
//		// if COMMIT command is used, we save this block's "block" HashMap to the "db" HashMap
//		// since this "block" HashMap includes the data from all parent blocks, there is no need to commit parent blocks separately
//
//		}
//		// return true, which will allow the beginNested() and begin() methods to exit
//		return true;
//	}



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

	public boolean commit(HashMap<String, Integer> block) {
		// to COMMIT this block's data, we replace "db" with the new version of the HashMap, which is "block"
		this.db = block;
		return true;
	}

}
