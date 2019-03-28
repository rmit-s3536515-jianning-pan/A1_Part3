import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;

public class Part3_2 {

	public static final int COMMAND_LENGTH = 3;
	public static final String FILE_PREFIX = "heap.";
	public static void main(String[] args) {
		// TODO Auto-generated method stub
			int pagesize;
			
			if(args.length!=COMMAND_LENGTH) {
				System.out.println("Insufficent parameters are provided! Please follow : dbquery text pagesize");
				System.exit(1);
			}
			else {
				if(args[0].equals("dbquery") && Part3.isNumeric(args[2])) { //check the first para and third 
					pagesize = Integer.parseInt(args[2]); 
					String inputfile = FILE_PREFIX + pagesize;
					File f = new File(inputfile);
					if(f.exists()) {
						SearchTTextFromFile(inputfile,args[1]);
					}
					else {
						System.out.println("The file with pagesize of "+pagesize + " is not available");
					}
					
					
				}
				else System.out.println("Incorrect Parameters are typed! Please follow : dbquery text pagesize");
			}
		
	}

	

	public static void SearchTTextFromFile(String inputfilename,String text) {
		//reading the file
		long start = System.currentTimeMillis();
		FileInputStream fin;
		ObjectInputStream ois;
		try {
			fin = new FileInputStream(inputfilename);
			ois = new ObjectInputStream(fin);
//			int count = 1;
			Object obj = null;
			String db_field = null;
			while ((obj = ois.readObject()) != null) {
				
			      if (obj instanceof DataRecord) {
			    	  DataRecord record = (DataRecord) obj;
			    	  db_field = record.getDeviceId()+"_"+record.getArrivalTime();
			    	  if(db_field.contains(text)) {
			    		  System.out.println(record);
			    	  }
			        

			      }
//			      else {
//			    	  System.out.print(obj);
//			      }

			}
			long end = System.currentTimeMillis();
			System.out.println("The time taken to perform Search Operation is " + (end-start)/1000.0 + " seconds");
			
			ois.close();
			fin.close();
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
	}
}
