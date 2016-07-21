import java.sql.*;

public class OLTPBench {
	static final int nRecords = 100000;
	static final int nIterations = 100000;
	static final int nTables = 4;

	static boolean random = true;

	static class Barrier { 
		int count;

		public Barrier(int members) { 
			count = members;
		}
		
		public synchronized void reach() throws InterruptedException 
		{
			if (--count != 0) { 
				wait();
			} else {
				notifyAll();
			}
		}
	}

	static Barrier barrier = new Barrier(nTables);

	static class Job extends Thread { 
		public void run() { 
			try { 
				Connection con = DriverManager.getConnection(url, user, password);
				java.util.Random rnd = new java.util.Random();
				Statement stmt = con.createStatement(); 
				ResultSet rs = stmt.executeQuery("select count(*) from t" + tid);
				rs.next();
				if (rs.getLong(1) == 0) { 
					PreparedStatement pstmt = con.prepareStatement("insert into  t" + tid + " values (?)");
					for (int i = 0; i < nRecords; i++) { 
						pstmt.setInt(1, i);
						pstmt.executeUpdate();
					}
				}
				PreparedStatement pstmt[] = new PreparedStatement[nTables];
				for (int i = 0; i < nTables; i++) {
					pstmt[i] = con.prepareStatement("select * from t" + i + " where k=?");
				} 
				barrier.reach();
				long start = System.currentTimeMillis();
				for (int i = 0; i < nIterations; i++) {
					int s = random ? rnd.nextInt(nTables) : tid;
					pstmt[s].setInt(1, rnd.nextInt(nRecords));
					rs = pstmt[s].executeQuery();
					rs.next();
					rs.close();
				}
				System.out.println("Thread " + tid + " query time: " + (System.currentTimeMillis() - start) + " msec");
				con.close();
			} catch (Exception x) { 
				x.printStackTrace();
			}
		}
		public Job(int tid, String url, String user, String password) { 
			this.tid = tid;
			this.url = url;
			this.user = user;
			this.password = password;
		}
		int    tid;
		String url;
		String user;
		String password;
	}

	public static void initializeDatabase(String url, String user, String password) throws Exception 
	{ 
		Connection con = DriverManager.getConnection(url, user, password);
		Statement stmt = con.createStatement(); 
		stmt.executeUpdate("set max_heap_table_size=4*1024*1024*1024");

		for (int i = 0; i < nTables; i++) {
			stmt.execute("create table if not exists t" + i + "(k integer primary key) ENGINE=MEMORY");			
		}
		con.close();
	}

	public static void main(String[] args) throws Exception 
	{
        String db = (args.length != 0) ? args[0].toLowerCase() : "mysql";
        String driver;
        String url;
        String username;
        String password;
        if (db.startsWith("monet")) {
            driver = "nl.cwi.monetdb.jdbc.MonetDriver";
            url = "jdbc:monetdb://localhost/demo";
            username = "monetdb";
            password = "monetdb";
        } else if (db.startsWith("sqlite")) {
            driver = "org.sqlite.JDBC";
            url = "jdbc:sqlite::memory:";
            username = "sqlite";
            password = "sqlite";     
        } else if (db.startsWith("mysql")) {
            driver = "com.mysql.jdbc.Driver";
            url = "jdbc:mysql://localhost/test";
            username = "mysql";
            password = null;     
        } else if (db.startsWith("postgre")) {
            driver = "org.postgresql.Driver";
            url = "jdbc:postgresql:postgres";
            username = "postgres";
            password = "postgres";                
        } else {
            throw new IllegalArgumentException("Unsupported database");
        }             
		Class.forName(driver);
		
		initializeDatabase(url, username, password);
			
		Job[] jobs = new Job[nTables];
		for (int i = 0; i < nTables; i++) { 
			jobs[i] = new Job(i, url, username, password);
		}
        long start = System.currentTimeMillis();
		for (int i = 0; i < nTables; i++) { 		
			jobs[i].start();
		}
		for (int i = 0; i < nTables; i++) { 		
			jobs[i].join();
		}
        System.out.println("Elapsed time query execution: " + (System.currentTimeMillis() - start) + " msec");

    }
}

