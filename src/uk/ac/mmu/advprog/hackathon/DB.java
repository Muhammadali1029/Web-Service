package uk.ac.mmu.advprog.hackathon;

import java.io.StringWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.json.JSONArray;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

/**
 * Handles database access from within your web service
 * @author You, Mainly!
 */
public class DB implements AutoCloseable 
{
	//allows us to easily change the database used
	private static final String JDBC_CONNECTION_STRING = "jdbc:sqlite:./data/AMI.db";
	
	//allows us to re-use the connection between queries if desired
	private Connection connection = null;
	
	/**
	 * Creates an instance of the DB object and connects to the database
	 */
	public DB() {
		try {
			connection = DriverManager.getConnection(JDBC_CONNECTION_STRING);
		}
		catch (SQLException sqle) {
			error(sqle);
		}
	}
	 
	
	
	/**
	 * Returns the number of entries in the database, by counting rows
	 * @return The number of entries in the database, or -1 if empty
	 */
	public int getNumberOfEntries() 
	{
		int result = -1;
		try {
			Statement s = connection.createStatement();
			ResultSet results = s.executeQuery("SELECT COUNT(*) AS count FROM ami_data");
			while(results.next()) { //will only execute once, because SELECT COUNT(*) returns just 1 number
				result = results.getInt(results.findColumn("count"));
			}
		}
		catch (SQLException sqle) {
			error(sqle);
			
		}
		
		finally 
		{
			//close the connection if possible
			if (connection != null)
			{
				try 
				{
					connection.close();
				}
				catch (SQLException sqe)
				{
					//GIVE UP
				}
			}
		}
		
		return result;
	}
	
	/**
	 *Returns the last signals shown except OFF NR BLNK
	 * 
	 * @param signal_id		entered in the url 
	 */
	
	public String lastSignal(String signal_id)
	{
		String result = null; //create a null string to use later
		try 
		{
			PreparedStatement s = connection.prepareStatement("SELECT signal_value FROM ami_data\r\n"
					+ "WHERE signal_id = ?\r\n"
					+ "AND NOT signal_value = \"OFF\"\r\n"
					+ "AND NOT signal_value = \"NR\"\r\n"
					+ "AND NOT signal_value = \"BLNK\"\r\n"
					+ "ORDER BY datetime DESC\r\n"
					+ "LIMIT 1;");
			
			s.setString(1, signal_id); //places signal_id into the first ?
			ResultSet results = s.executeQuery(); //executes query
			
			//iterate over results
			while (results.next()) 
			{
				result = results.getString("signal_value"); //gets value of signal_value and puts it in result
			}
		}
		
		catch (SQLException sqle) 
		{
			error(sqle);
		}
		
		finally 
		{
			//close the connection if possible
			if (connection != null)
			{
				try 
				{
					connection.close();
				}
				catch (SQLException sqe)
				{
					//GIVE UP
				}
			}
		}
		
		return result; //return final value of result
	} 
	
	
	/**
	 *Returns the number of times each signal has been shown
	 * 
	 * @param signal_id		entered in the url 
	 */
	public String frequency(String signal_id)
	{
		String d = "[]"; //creats an empty array of strings
		
		try 
		{
			PreparedStatement s = connection.prepareStatement("SELECT\r\n"
					+ "COUNT(signal_value) AS frequency, \r\n"
					+ "signal_value \r\n"
					+ "FROM ami_data \r\n"
					+ "WHERE signal_id LIKE ?\r\n"
					+ "GROUP BY signal_value \r\n"
					+ "ORDER BY frequency DESC;");
			s.setString(1, signal_id); //places signal_id into the first ?
			ResultSet results = s.executeQuery(); //executes query
			
			JSONArray ja  = new JSONArray(); //creating a json array

			//itterates over resaults
			while (results.next())
			{	
				JSONObject signals = new JSONObject();
				
				//putting values for signal_value and frequency
				signals.put("Value: ", results.getString("signal_value")); 
				signals.put("Frequency: ", results.getString("frequency"));
				ja.put(signals);	//puts signals in json array
			}
		
			d = ja.toString();
		}
		catch (SQLException sqle) 
		{
			error(sqle);
		}
		
		finally 
		{
			//close the connection if possible
			if (connection != null)
			{
				try 
				{
					connection.close();
				}
				catch (SQLException sqe)
				{
					//GIVE UP
				}
			}
		}
		
		return d;
	}
	
	/**
	 *Returns all the groups of signals
	 * 
	 *
	 */
	
	public String groups()
	{
		String groupsOutput = "[]"; 
		try 
		{	
			PreparedStatement s = connection.prepareStatement( "SELECT DISTINCT signal_group FROM ami_data");
			
			ResultSet results = s.executeQuery();
			
			
			try
			{
				//same as reading xml
				DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
				Document doc = dbf.newDocumentBuilder().newDocument();
				
				//Create the tree
				Element groups =  doc.createElement("Groups");
				doc.appendChild(groups);
				
				while (results.next())
				{
					Element group = doc.createElement("Group"); //creting Group element
					group.setTextContent(results.getString("signal_group")); //seting values
					groups.appendChild(group); //making it a child of Groups
				}

				//ALL THIS TO MAKE IT INTO A STRING
				Transformer transformer = TransformerFactory.newInstance().newTransformer();
				Writer output = new StringWriter();
				transformer.setOutputProperty(OutputKeys.INDENT, "yes");
				transformer.transform(new DOMSource(doc), new StreamResult(output));
				System.out.println(output);
				
				groupsOutput = output.toString(); 
			}
			catch (ParserConfigurationException | TransformerException ioe)
			{
				System.out.println("Error creating XML: " + ioe);
			}
			
		}
		catch (SQLException sqle) 
		{
			error(sqle);
		}
		
		finally 
		{
			//close the connection if possible
			if (connection != null)
			{
				try 
				{
					connection.close();
				}
				catch (SQLException sqe)
				{
					//GIVE UP
				}
			}
		}
		
		return groupsOutput;
	}
	
	
	/**
	 *Returns all the signals shown at the specified motorway and date and time
	 * 
	 * @param signal_group		entered in the url and gives motorway
	 * @param datetime			entered in url and gives thge date and time for the signal parameter
	 */
	
	public String signalsAtTime(String signal_group, String datetime)
	{
		String signalsOutput = "[]";
		
		try
		{
			PreparedStatement s = connection.prepareStatement("SELECT datetime, signal_id, signal_value \r\n"
					+ "FROM ami_data \r\n"
					+ "WHERE\r\n"
					+ "signal_group = ?\r\n"
					+ "AND datetime < ?\r\n"
					+ "AND (datetime, signal_id) IN (\r\n"
					+ "SELECT MAX(datetime) AS datetime, signal_id\r\n"
					+ "FROM ami_data\r\n"
					+ "WHERE\r\n"
					+ "signal_group = ?\r\n"
					+ "AND datetime < ?\r\n"
					+ "GROUP BY signal_id\r\n"
					+ ")\r\n"
					+ "ORDER BY signal_id;");
			
			s.setString(1, signal_group);// first ?
			s.setString(2, datetime); // second ?
			s.setString(3, signal_group); // third ?
			s.setString(4, datetime); // fourth ?
			ResultSet results = s.executeQuery();
			
			try
			{
				DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
				Document doc = dbf.newDocumentBuilder().newDocument();
				
				//Create the tree
				Element signalsElement =  doc.createElement("Signals");
				doc.appendChild(signalsElement);
				
				while (results.next())
				{	
					Element signal = doc.createElement("Signal"); //create signal element 
					signalsElement.appendChild(signal); //make signal a child of signals
					
					Element id = doc.createElement("ID"); //create ID element 
					id.setTextContent(results.getString("signal_id")); //put text into id
					signal.appendChild(id); //make id a child of signal
					
					Element dateset = doc.createElement("DateSet"); //create date set element
					dateset.setTextContent(results.getString("datetime")); //put date time into it
					signal.appendChild(dateset); // make it a child of signal
					
					Element value = doc.createElement("Value"); //create value element
					value.setTextContent(results.getString("signal_value"));
					signal.appendChild(value);
				}
				
				//ALL THIS TO MAKE IT INTO A STRING
				Transformer transformer = TransformerFactory.newInstance().newTransformer();
				Writer output = new StringWriter();
				transformer.setOutputProperty(OutputKeys.INDENT, "yes");
				transformer.transform(new DOMSource(doc), new StreamResult(output));
				System.out.println(output);
				
				signalsOutput = output.toString(); 
				
			}
			
			catch (ParserConfigurationException | TransformerException ioe)
			{
				System.out.println("Error creating XML: " + ioe);
			}
		}
		
		catch (SQLException sqle) 
		{
			error(sqle);
		}
		
		finally 
		{
			//close the connection if possible
			if (connection != null)
			{
				try 
				{
					connection.close();
				}
				catch (SQLException sqe)
				{
					//GIVE UP
				}
			}
		}

		return signalsOutput;
	}
	
	
	//Closes the connection to the database, required by AutoCloseable interface.
	@Override
	public void close() {
		try {
			if ( !connection.isClosed() ) {
				connection.close();
			}
		}
		catch(SQLException sqle) {
			error(sqle);
		}
	}

	/**
	 * Prints out the details of the SQL error that has occurred, and exits the programme
	 * @param sqle Exception representing the error that occurred
	 */
	private void error(SQLException sqle) {
		System.err.println("Problem Opening Database! " + sqle.getClass().getName());
		sqle.printStackTrace();
		System.exit(1);
	}
}
