package uk.ac.mmu.advprog.hackathon;
import static spark.Spark.get;
import static spark.Spark.port;
import static spark.Spark.*;

import spark.Request;
import spark.Response;
import spark.Route;
import java.sql.*;

/**
 * Handles the setting up and starting of the web service
 * You will be adding additional routes to this class, and it might get quite large
 * Feel free to distribute some of the work to additional child classes, like I did with DB
 * @author You, Mainly!
 */
public class AMIWebService {

	/**
	 * Main program entry point, starts the web service
	 * @param args not used
	 */
	public static void main(String[] args) {		
		port(8088);
		
		System.setProperty("org.eclipse.jetty.util.log.announce", "false");
		System.setProperty("org.eclipse.jetty.LEVEL", "OFF");
		
		
		//TEST
		get("/test", new Route() {
			@Override
			public Object handle(Request request, Response response) throws Exception {
				try (DB db = new DB()) {
					return "Number of Entries: " + db.getNumberOfEntries();
				}
			}			
		}); 
		
		//LASTSIGNAL
		get ("/lastsignal", new Route()
		{
			@Override
			public Object handle(Request request, Response response) throws Exception
			{
				String lastSignal = request.queryParams("signal_id");
				
				try (DB db = new DB())
				{
					//return database.test(); 
					return db.lastSignal(lastSignal);
				}
			}
		});
		
		//FREQUENCY
		get ("/frequency", new Route()
		{
			@Override
			public Object handle(Request request, Response response) throws Exception
			{
				String motorway = request.queryParams("motorway");
				response.type("application/json");
				try (DB db = new DB())
				{
					return db.frequency(motorway + "%");
				}
			}
		});
		
		//GROUPS
		get ("/groups", new Route()
		{
			@Override
			public Object handle(Request request, Response response) throws Exception
			{
				response.type("application/xml");
				try (DB db = new DB())
				{
					return db.groups();
				}
			}
		});
		
		//SIGNALATTIME
		get ("/signalsattime", new Route()
		{
			@Override
			public Object handle(Request request, Response response) throws Exception
			{
				String signalGroup = request.queryParams("group");
				String dateTime = request.queryParams("time");
				response.type("application/xml");
				try (DB db = new DB())
				{
					return db.signalsAtTime(signalGroup, dateTime);
				}
			}
		});
		
		notFound ("No Results!"); // prints no result when wrong parameters are entered for test or lastsignal
	}
	
	

}
