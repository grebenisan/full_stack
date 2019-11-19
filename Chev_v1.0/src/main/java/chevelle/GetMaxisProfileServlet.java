/**************************************************************************
 * Servlet: GetMaxisProfileServlet
 * URL: /GetMaxisProfile
 * Call method (preferred): GET
 * Parameters:
 *  - ColRegId: the ID of the column-regexp association
 *  - format: the output format of the response (json, text)
 * Author: Dan Grebenisan
 * History: - Mar 2018 - Created
 *          - Mar 26, 2018 - Changed the HTTP error codes
 *          - Mar 27, 2018 - return only the first record
 *          - Apr 16, 2018 - added code for the currently running environment
 ***************************************************************************/
package chevelle;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
// import javax.sql.DataSource;
import org.apache.tomcat.jdbc.pool.DataSource;

import com.google.gson.*;

/**
 *
 * @author BZY91Q
 */
public class GetMaxisProfileServlet extends HttpServlet {

    
    // Context initContext = null;
    // Context envContext = null;
    // DataSource ds = null;

    javax.naming.InitialContext initContext = null;
    javax.naming.Context envContext = null;
    ServletContext srvContext = null;
    
    org.apache.tomcat.jdbc.pool.DataSource ds = null;
    
    String CHEVELLE_ENV = null;
    String sys_env_var = null;
    String init_context_env = null;

    ChevelleEnvironment chevelleEnvironment = null;
    ChevelleHelper chevelleHelper = null;        

    @Override
    public void init() throws ServletException 
    {

        srvContext = getServletContext();
        
        try
        {
            
            CHEVELLE_ENV = (java.lang.String) srvContext.getAttribute("CHEVELLE_ENV");
            log("Chevelle environment successfully retrieved from the servlet context: " + CHEVELLE_ENV);
        }
        catch (Exception e)
        {
            log("Cannot retrieve the CHEVELLE_ENV: " + e.getMessage());
            CHEVELLE_ENV = null;
        }
        
        if ( CHEVELLE_ENV == null || CHEVELLE_ENV.equals("") || CHEVELLE_ENV.equals("null") )
        {
            // try reading the environment variable
            try
            {
                sys_env_var = System.getenv("CHEVELLE_ENV");
                log("Chevelle system variable look-up success: " + sys_env_var);
            }
            catch (Exception e)
            {
                log("Chevelle system variable look-up error: " + e.getMessage());
                sys_env_var = null;
            }               
            
            if (sys_env_var != null && !sys_env_var.equals(""))
            {
                CHEVELLE_ENV = sys_env_var; // a local environment variable would overwrite the context variable
                log("Chevelle environment overwrite with system variable: " + CHEVELLE_ENV);
            }
            else // didn't get anything from the system environment, try the system environment variable
            {
                log("Chevelle local system variable is null");
                // now try to read the variable from the initial context
                try
                {
                    initContext = new InitialContext();
                    envContext = (Context)initContext.lookup("java:/comp/env");

                    init_context_env = (String) envContext.lookup("CHEVELLE_ENV");
                    if(init_context_env != null && !init_context_env.equals("") )
                    {
                        CHEVELLE_ENV = init_context_env;
                        log("Chevelle initial context look-up success: " + init_context_env);
                    }
                    else
                    {
                        CHEVELLE_ENV = null;
                        log("Chevelle initial context look-up null or empty ");
                    }
                }
                catch (Exception e)
                {
                    log("Chevelle initial context look-up error: " + e.getMessage());
                    CHEVELLE_ENV = null;
                    init_context_env = null;
                }                
            }        
            
            // at this point if CHEVELLE_ENV is still null, just initialize it with the default for PRODUCTION
            if(CHEVELLE_ENV == null || CHEVELLE_ENV.equals("") || CHEVELLE_ENV.equals("null"))
                CHEVELLE_ENV = "PRD";  // the last resort
            
        } // else, we're good, we have a value from the ServletContext
     
        // chevelleEnvironment.setEnvironment(CHEVELLE_ENV); // do I need this ?
        // chevelleHelper.setEnvironment(CHEVELLE_ENV);      // do I need this ?
        
        // the most important - getting the data source from the servlet context
        try
        {
            ds = (org.apache.tomcat.jdbc.pool.DataSource) srvContext.getAttribute("chevelle_connection_pool");
            log("Data Source successfully retrieved from the servlet context!");
        }
        catch (Exception e)
        {
            log("Error: Data Source could not be retrieved from the servlet context: " + e.getMessage());
        }
        
    }
    
    @Override
    public void destroy()
    {
            ds = null;
            try {envContext.close();}catch(Exception ignore){}finally{envContext = null;}
            try {initContext.close();}catch(Exception ignore){}finally{initContext = null;}        
    }
    
   
    
    /**
     * Processes requests for both HTTP <code>GET</code> and <code>POST</code>
     * methods.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException 
    {
        Connection con = null;
        Statement stmt = null;  // Or PreparedStatement if needed
        ResultSet rs = null;
        
        PrintWriter out = null;
        
        String ColRegId = request.getParameter("ColRegId");
        if(ColRegId == null)
            ColRegId = ""; // the TableID search       
        
        //get the format of the request
        String format = request.getParameter("format");
        if(format == null)
            format = "json"; // json, text 
        
        try
        {        
            out = response.getWriter();
        }
        catch(Exception ex)
        {
            log("Error: GetMaxisProfileServlet - Cannot open the out stream: " + ex.getMessage());
            response.setStatus(500);
            response.sendError(500);
            out = null;
            return;
        }        
        
        // set the response time depending the format
        if (format.equals("json"))
        {
            response.setContentType("application/json;charset=UTF-8");
        }
        else if (format.equals("text"))
            response.setContentType("text/plain;charset=UTF-8");
        else
        {
            format = "json";
            response.setContentType("application/json;charset=UTF-8");
        }        

        // read again the currently running environment from the servlet context
        // if different from the existing one, call init() to reload the new environment and the data source
        
        String latest_CHEVELLE_ENV = null;
        try
        {
            latest_CHEVELLE_ENV = (java.lang.String) srvContext.getAttribute("CHEVELLE_ENV");
        }
        catch (Exception e)
        {
            log("Cannot read the currently running environment of Chevelle: " + e.getMessage());
        }
        
        if(latest_CHEVELLE_ENV == null || !latest_CHEVELLE_ENV.equals(CHEVELLE_ENV) || latest_CHEVELLE_ENV.equals("") || latest_CHEVELLE_ENV.equals("null") )
        {
            init();
            log("Chevelle currently running environment and DB data source reloaded: " + CHEVELLE_ENV);
        }
        
        // get connection from DB
 	try 
	{
            con = ds.getConnection();
  
        }
        catch(Exception ex)
        {
            log("Error: GetMaxisProfileServlet cannot get connection: " + ex.getMessage());
	    out.println("Error: GetMaxisProfileServlet cannot get connection: " + ex.getMessage());            
            response.setStatus(401); // Need to change the error code
            response.sendError(401); // Need to change the error code
            return;                        
        }


 	try // prepare SQL, extract data , format and return the data
	{
   
            // build the SQL query
            stmt = con.createStatement();
            stmt.setQueryTimeout(120);
            
            String sql = " SELECT TOP 1 RecordCount, ExecutionDate, CAST(JsonResult as varchar(1000)) as JsonResult, Id ";
            sql += " from dbo.ChevelleStat where MasterColumnChevelleQueryId = " + ColRegId + " ";
            
            rs = stmt.executeQuery(sql);            
            int rec_count = 0;    
            
            //create string variables for each column
            String record_count = "";
            String execution_date = "";         
            String val_JsonResult = "";
            String val_Id = "";
            
            // create the strings for all the fields in the JsonResult
            String total_records = "";
            String unique_records = "";
            String column_count = "";
            String total_suspect_in_class = "";
            String default_count = "";
            String threshold_percent = "";
            String suspect_class_type = "";
            String length = "";
            String avg_length = "";
            String avg_value = "";
            String min_value = "";
            String max_value = "";       

            
            //String jsonresponse = "[ ";
            //String textresponse = "";
            
            String text_response = "";
            String json_response = "";
            String json_parse_error = "";

            while(rs.next())
            {
                rec_count++;                
            
                record_count = rs.getString("RecordCount");
                if (rs.wasNull())
                    record_count = "";
                else
                    record_count = record_count.trim();

                
                execution_date = rs.getString("ExecutionDate");
                if (rs.wasNull())
                    execution_date = "";
                else
                    execution_date = execution_date.trim();                
                             
                
                val_JsonResult = rs.getString("JsonResult");
                if (rs.wasNull())
                    val_JsonResult = "";
                else
                    val_JsonResult = val_JsonResult.trim();                                
                
                
                val_Id = rs.getString("Id");
                if (rs.wasNull())
                    val_Id = "";
                else
                    val_Id = val_Id.trim();                                      
                
                try // parse the JSON result
                {
                    JsonParser parser = new JsonParser();
                    JsonElement jsonTree = parser.parse(val_JsonResult);
                    if(jsonTree.isJsonObject()) 
                    {
                        JsonObject obj_jsonresult = jsonTree.getAsJsonObject();

                        total_records = obj_jsonresult.get("total_records").getAsString();
                        unique_records = obj_jsonresult.get("unique_records").getAsString();
                        column_count = obj_jsonresult.get("column_count").getAsString();
                        total_suspect_in_class = obj_jsonresult.get("total_suspect_in_class").getAsString();
                        default_count = obj_jsonresult.get("default_count").getAsString();
                        threshold_percent = obj_jsonresult.get("threshold_percent").getAsString();
                        suspect_class_type = obj_jsonresult.get("suspect_class_type").getAsString();
                        length = obj_jsonresult.get("length").getAsString();
                        avg_length = obj_jsonresult.get("avg_length").getAsString();
                        avg_value = obj_jsonresult.get("avg_value").getAsString();
                        min_value = obj_jsonresult.get("min_value").getAsString();        
                        max_value = obj_jsonresult.get("max_value").getAsString();

                    }
                    else //json null or not an object
                    {
                        total_records = "";
                        unique_records = "";
                        column_count = "";
                        total_suspect_in_class = "";
                        default_count = "";
                        threshold_percent = "";
                        suspect_class_type = "";
                        length = "";
                        avg_length = "";
                        avg_value = "";
                        min_value = "";        
                        max_value = "";
                    }
                }
                catch (Exception parse_ex)
                {
                    json_parse_error = parse_ex.getMessage();
                    break;
                    // out.println("{ \"Cannot parse JsonResult\": \"" + parse_ex + "\" }");
                }                                            

                // put the profiling stats first in the response, then the fields from the JsonResult
                json_response = "{ \"record_count\": \"" + record_count + "\", " +
                "\"execution_date\": \"" + execution_date + "\", " +
                "\"total_records\": \"" + total_records + "\", " + 
                "\"unique_records\": \"" + unique_records + "\", " +
                "\"column_count\": \"" + column_count + "\", " +
                "\"total_suspect_in_class\": \"" + total_suspect_in_class + "\", " +
                "\"default_count\": \"" + default_count + "\", " +
                "\"threshold_percent\": \"" + threshold_percent + "\", " +
                "\"suspect_class_type\": \"" + suspect_class_type + "\", " +
                "\"length\": \"" + length + "\", " +
                "\"avg_length\": \"" + avg_length + "\", " +
                "\"avg_value\": \"" + avg_value + "\", " +
                "\"min_value\": \"" + min_value + "\", " +
                "\"max_value\": \"" + max_value + "\", " +
                "\"profile_id\": \"" + val_Id + "\" }";
                
                text_response = "record_count: " + record_count + ", \n" +
                "execution_date: " + execution_date + ", \n" +
                "total_records: " + total_records + ", \n" + 
                "unique_records: " + unique_records + ", \n" +
                "column_count: " + column_count + ", \n" +
                "total_suspect_in_class: " + total_suspect_in_class + ", \n" +
                "default_count: " + default_count + ", \n" +
                "threshold_percent: " + threshold_percent + ", \n" +
                "suspect_class_type: " + suspect_class_type + ", \n" +
                "length: " + length + ", \n" +
                "avg_length: " + avg_length + ", \n" +
                "avg_value: " + avg_value + ", \n" +
                "min_value: " + min_value + ", \n" +
                "max_value: " + max_value + ", \n" +
                "profile_id: " + val_Id + " \n";
                
            } // browsing the records
            
            if(format.equals("json"))
            {
                if(json_parse_error.equals("")) // no error
                {
                    if (json_response.equals(""))
                        out.println("{}");
                    else
                        out.println(json_response);
                }
                else // parsing error
                {
                    json_response = "{ \"record_count\": \"" + record_count + "\", " +
                    "\"execution_date\": \"" + execution_date + "\", " +
                    "\"total_records\": \"" + json_parse_error + "\", " + 
                    "\"unique_records\": \"" + json_parse_error + "\", " +
                    "\"column_count\": \"" + json_parse_error + "\", " +
                    "\"total_suspect_in_class\": \"" + json_parse_error + "\", " +
                    "\"default_count\": \"" + json_parse_error + "\", " +
                    "\"threshold_percent\": \"" + json_parse_error + "\", " +
                    "\"suspect_class_type\": \"" + json_parse_error + "\", " +
                    "\"length\": \"" + json_parse_error + "\", " +
                    "\"avg_length\": \"" + json_parse_error + "\", " +
                    "\"avg_value\": \"" + json_parse_error + "\", " +
                    "\"min_value\": \"" + json_parse_error + "\", " +
                    "\"max_value\": \"" + json_parse_error + "\", " +
                    "\"profile_id\": \"" + val_Id + "\" }";

                    out.println(json_response);
                }
            }
            
            if (format.equals("text"))
            {
                if(json_parse_error.equals("")) // no error
                {
                    out.println(text_response);
                }
                else // parsing error
                {
                    text_response = "record_count: " + record_count + ", \n" +
                    "execution_date: " + execution_date + ", \n" +
                    "total_records: " + json_parse_error + ", \n" + 
                    "unique_records: " + json_parse_error + ", \n" +
                    "column_count: " + json_parse_error + ", \n" +
                    "total_suspect_in_class: " + json_parse_error + ", \n" +
                    "default_count: " + json_parse_error + ", \n" +
                    "threshold_percent: " + json_parse_error + ", \n" +
                    "suspect_class_type: " + json_parse_error + ", \n" +
                    "length: " + json_parse_error + ", \n" +
                    "avg_length: " + json_parse_error + ", \n" +
                    "avg_value: " + json_parse_error + ", \n" +
                    "min_value: " + json_parse_error + ", \n" +
                    "max_value: " + json_parse_error + ", \n" +
                    "profile_id: " + val_Id + " \n";
                    
                    out.println(text_response);
                }
            }
            out.flush();
            
            // Clean-up environment
            rs.close();
            rs = null;
            stmt.close();
            stmt = null;
            try {con.close();} //Connection released to the Connection Pool
            catch(Exception e)
            {
                out.print("GetMaxisProfileServlet: Cannot close and release Pool Connection: " + e.getMessage());
                log("GetMaxisProfileServlet: Cannot close and release connection to the Pool: " + e.getMessage());
            } // If connection pooling, return to connection pool; DO NOT set to null                     

        } // the BIG try to prepare SQL, extract data , format and return the data
        catch(SQLException ex)
        {
            log("Error: GetMaxisProfileServlet - SQL exception: " + ex.getMessage());
	    out.println("Error: connecting or executing database command: " + ex.getMessage());            
            response.setStatus(409); // Need to change the error code
            // response.sendError(409); // Need to change the error code
            // return;            
        }
        catch (Exception e) 
        {
            log("Error: GetMaxisProfile servlet major error: " + e.getMessage());
	    out.println("Error: GetMaxisProfile servlet major error: " + e.getMessage());            
            response.setStatus(500); // Need to change the error code
            // response.sendError(500); // Need to change the error code            
            // return;
	} 
	finally 
        {
                // Always make sure result sets and statements are closed,
                // and the connection is returned to the pool
            if (rs != null) 
            {
                try { rs.close(); } 
                catch (SQLException ex) { ; }
                rs = null;
            }
            if (stmt != null) 
            {
                try { stmt.close(); } 
                catch (SQLException ex) { ; }
                stmt = null;
            }

        }            
            

        
    }

    // <editor-fold defaultstate="collapsed" desc="HttpServlet methods. Click on the + sign on the left to edit the code.">
    /**
     * Handles the HTTP <code>GET</code> method.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
    }

    /**
     * Handles the HTTP <code>POST</code> method.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
    }

    /**
     * Returns a short description of the servlet.
     *
     * @return a String containing servlet description
     */
    @Override
    public String getServletInfo() {
        return "Short description";
    }// </editor-fold>

}
