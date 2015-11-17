import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;

/*
*  By Steven Molitor
*
*  To compile this code, use the terminal command:
*  	javac My_DB.java
*  from within the directory the java file is in.
*
*  The program is then started with the command:
*		java My_DB
*
*  You also have the ability to use a .txt file as input with this command:
*		java My_DB < /path/to/file.txt
*
*/
public class My_DB {
	private HashMap<String, Integer> db = new HashMap<String, Integer>();  // will store all committed data
	private HashMap<Integer, Integer> counts = new HashMap<Integer, Integer>(); // will store record of how many times each value is present
	private Scanner in;

	public static void main(String args[]) {
		My_DB database = new My_DB();
		database.in = new Scanner(System.in);

		// this while loop will run until the END command stops the program
		while (true) {
			String line = database.in.nextLine(); // will hold the entire current command
			boolean success = false; // will be used to check if a valid command was run or not
			Scanner current = new Scanner(line);
			String command = null; // will hold the name of the current command
			String name = null; // will hold the name parameter of the current command
			Integer number = null; // will hold the value parameter of the current command

			if (current.hasNext()) {
				command = current.next();

				// these conditionals allow us to run the current command
				if (command.contains("UNSET")) {
					if (current.hasNext()) name = current.next();
					if (String.class.isInstance(name)) success = database.unset(name);
					if (success == false) database.invalid();

				} else if (command.contains("SET")) {
					if (current.hasNext()) name = current.next();
					if (current.hasNextInt() && String.class.isInstance(name)) success = database.set(name, current.nextInt());
					if (success == false) database.invalid();

				} else if (command.contains("GET")) {
					if (current.hasNext()) name = current.next();
					if (String.class.isInstance(name)) success = database.get(name);
					if (success == false) database.invalid();

				} else if (command.contains("NUMEQUALTO")) {
					if (current.hasNextInt()) {
						number = current.nextInt();
						success = database.numequalto(number);
					}
					if (success == false) database.invalid();

				} else if (command.contains("END")) {
					database.end();

				} else if (command.contains("BEGIN")) {
					HashMap<String, Integer> block = new HashMap<String, Integer>(); // initializing blank HashMap that will store new/updated values within the block
					HashSet<String> unsets = new HashSet<String>(); // initializing a blank HashSet that will keep track of UNSET commands within the block
					HashMap<Integer, Integer> count = new HashMap<Integer, Integer>(); // initializing a blank HashMap to keep track of how many times each value is present within the block
					database.begin(block, unsets, count);

				} else if (command.contains("ROLLBACK") || line.contains("COMMIT")) {
					// ROLLBACK and COMMIT have no effect when called here, outside of a transaction block
					System.out.println("NO TRANSACTION");

				} else {
					database.invalid();

				}

			} else { // occurs if user inputs a blank command
				database.invalid();
			}
			current.close();
		}

	}

	// carries out the SET command
	public boolean set(String name, Integer value) {
		// if this name already had a previous value, we must decrement the count of the old value
		if (this.db.containsKey(name)) {
			Integer number = this.db.get(name);
			this.removeOrDecrement(number, this.counts);
		}

		this.db.put(name, value);
		this.putOrIncrement(value, this.counts);
		return true;
	}

	// carries out the GET command
	public boolean get(String name) {
		System.out.println(this.db.get(name));
		return true;
	}

	// carries out the UNSET command
	public boolean unset(String name) {
		Integer number = this.db.get(name);
		this.db.remove(name);
		if (number == null) return false;
		this.removeOrDecrement(number, this.counts);
		return true;
	}

	// carries out the NUMEQUALTO command
	public boolean numequalto(Integer value) {
		Integer answer = this.counts.get(value);
		if (answer == null) {
			System.out.println(0);
		} else {
			System.out.println(answer);
		}
		return true;
	}

	// carries out the END command
	public void end() {
		this.in.close();
		System.exit(0);
	}

	// used to keep track of the count of a given value
	public void removeOrDecrement(Integer number, HashMap<Integer, Integer> counts) {
		// while not in a transaction block, a value's count can never get below 1
		if (counts.get(number) <=1 ) {
			counts.remove(number);
		} else {
			Integer currentValue = counts.get(number);
			counts.put(number, currentValue-1);
		}
	}

	// used to keep track of the count of a given value
	// used both inside and outside of transaction blocks
	public void putOrIncrement(Integer number, HashMap<Integer, Integer> counts) {
		if (!counts.containsKey(number)) {
			counts.put(number, 1);
		} else {
			Integer currentValue = counts.get(number);
			counts.put(number,  currentValue+1);
		}
	}

	// notifies user of invalid input
	public void invalid() {
		System.out.println("INVALID INPUT");
	}

	// carries out the BEGIN command, and is used recursively for any nested transaction blocks
	public boolean begin(HashMap<String, Integer> parent, HashSet<String> parentUnsets, HashMap<Integer, Integer> parentCounts) {
		boolean exit = false;
		HashMap<String, Integer> block = new HashMap<String, Integer>(parent);
		HashSet<String> blockUnsets = new HashSet<String>(parentUnsets);
		HashMap<Integer, Integer> blockCounts = new HashMap<Integer, Integer>(parentCounts);

		while (exit == false) {
			String line = this.in.nextLine();
			boolean success = false;
			Scanner current = new Scanner(line);
			String command = null;
			String name = null;
			Integer number = null;

			if (current.hasNext()) {
				command = current.next();

				// using conditionals to select command, as in main method
				if (command.contains("UNSET")) {
					if (current.hasNext()) name = current.next();
					if (String.class.isInstance(name)) success = this.unset(name, block, blockUnsets, blockCounts);
					if (success == false) this.invalid();

				} else if (command.contains("SET")) {
					if (current.hasNext()) name = current.next();
					if (current.hasNextInt() && String.class.isInstance(name)) success = this.set(name, current.nextInt(), block, blockUnsets, blockCounts);
					if (success == false) this.invalid();

				} else if (command.contains("GET")) {
					if (current.hasNext()) name = current.next();
					if (String.class.isInstance(name)) success = this.get(name, block, blockUnsets);
					if (success == false) this.invalid();

				} else if (command.contains("NUMEQUALTO")) {
					if (current.hasNextInt()) {
						number = current.nextInt();
						success = this.numequalto(number, block, blockCounts);
					}
					if (success == false) this.invalid();

				} else if (command.contains("END")) {
					this.end();

				} else if (command.contains("BEGIN")) {
					exit = this.begin(block, blockUnsets, blockCounts);

				} else if (command.contains("COMMIT")) {
					exit = this.commit(block, blockUnsets, blockCounts);

				} else if (command.contains("ROLLBACK")) {
					// to ROLLBACK, we simply return false, negating any changes in this block but allowing parent blocks to continue to be manipulated
					current.close();
					return false;

				} else {
					this.invalid();
				}

			} else {
				this.invalid();
			}
			current.close();
		}

		// returning true will cause any begin() methods to exit
		return true;

	}

	// below are methods that are used from within transaction blocks

	// carries out SET command inside transaction blocks
	public boolean set(String name, Integer value, HashMap<String, Integer> block, HashSet<String> blockUnsets, HashMap<Integer, Integer> blockCounts) {
		// if this name already had a previous value, we must decrement the count of the old value
		if (block.containsKey(name)) {
			Integer number = block.get(name);
			this.removeOrDecrementBlock(number, blockCounts);
		}

		block.put(name,  value);
		if (blockUnsets.contains(name)) blockUnsets.remove(name); // this undoes a previous UNSET of this name from within an uncommitted block
		this.putOrIncrement(value, blockCounts);
		return true;
	}

	// carries out GET command inside transaction blocks
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

	// carries out UNSET command inside transaction blocks
	public boolean unset(String name, HashMap<String, Integer> block, HashSet<String> blockUnsets, HashMap<Integer, Integer> blockCounts) {
		// if we've already UNSET this name in this transaction block, there is no need to do it again
		// the UNSET command is still valid in this case, so we return true, but since there is nothing to unset, the rest of this method is not run
		if (blockUnsets.contains(name)) return true;

		// reduce the count of the value associated with the key
		Integer number = block.get(name);
		if (number == null) number = this.db.get(name); // if we are using UNSET on a string that was SET outside of this transaction block or its parent transaction blocks
		if (number == null) return false; // if we are trying to UNSET a string that is not set in either the db HashMap or the block HashMap
		this.removeOrDecrementBlock(number, blockCounts);

		// remove the value within the block, and mark it as a name that's been UNSET
		block.remove(name);
		blockUnsets.add(name); // this allows us to keep track of this UNSET before it is committed
		return true;
	}

	// carries out NUMEQUALTO command inside transaction blocks
	public boolean numequalto(Integer value, HashMap<String, Integer> block, HashMap<Integer, Integer> blockCounts) {
		Integer answerDB = this.counts.get(value);
		Integer answerBlock = blockCounts.get(value);

		if (answerDB == null && answerBlock == null) {
			System.out.println(0);
		} else if (answerDB == null) {
			System.out.println(answerBlock);
		} else if (answerBlock == null) {
			System.out.println(answerDB);
		} else {
			System.out.println(answerDB + answerBlock);
		}
		return true;
	}

	// carries out COMMIT command inside transaction blocks
	public boolean commit(HashMap<String, Integer> block, HashSet<String> blockUnsets, HashMap<Integer, Integer> blockCounts) {
		// to COMMIT this block's data, we SET all the items in block to db and UNSET all the names in blockUnsets
		Iterator<Entry<String, Integer>> updateSets = block.entrySet().iterator();
		Iterator<String> updateUnsets = blockUnsets.iterator();

		while (updateSets.hasNext()) {
			Map.Entry<String, Integer> current = (Map.Entry<String, Integer>)updateSets.next();
			this.set(current.getKey(), current.getValue());  // updates the db HashMap with the new/changed entries
		}

		while (updateUnsets.hasNext()) {
			String currentUnset = updateUnsets.next();
			this.unset(currentUnset); // runs the UNSET command on this string in the db HashMap, making the block's change permanent
		}

		return true;
	}

	// used to keep track of the count of a given value within a transaction block
	public void removeOrDecrementBlock(Integer number, HashMap<Integer, Integer> counts) {
		// inside a transaction block, we can have negative count values because we may have removed values that had been set in a parent block
		if (!counts.containsKey(number)) {
			counts.put(number, -1);
		} else {
			Integer currentValue = counts.get(number);
			counts.put(number, currentValue-1);
		}
	}

}
