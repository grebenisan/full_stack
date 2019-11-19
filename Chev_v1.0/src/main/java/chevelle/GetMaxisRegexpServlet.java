/**************************************************************************
 * Servlet: GetMaxisRegexpServlet
 * URL: /GetMaxisRegexp
 * Call method (prefered): GET
 * Parameters:
 *  - format: the output format of the response (json, text)
 * Author: Dan Grebenisan
 * History: - Mar 2018 - Created
 *          - Mar 26, 2018 - Changed the HTTP error codes
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

/**
 *
 * @author BZY91Q
 */
public class GetMaxisRegexpServlet extends HttpServlet 
{

    
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
            log("Error: GetMaxisRegexpServlet - Cannot open the out stream: " + ex.getMessage());
            response.setStatus(500);
            response.sendError(500);
            out = null;
            return;
        }        
        
//        out.println("TableId: " + str_TableId);
//        out.println("format: " + format);    
        
        // set the response time depending the format
        if (format.equals("json"))
        {
            response.setContentType("application/json;charset=UTF-8");
            out.println("[");
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
            // log("connection suuceeesfully retrieved from Connection Pool");        
        }
        catch(Exception ex)
        {
            log("Error: GetMaxisRegexpServlet cannot get connection: " + ex.getMessage());
	    out.println("Error: GetMaxisRegexpServlet cannot get connection: " + ex.getMessage());            
            response.setStatus(401); // Need to change the error code
            response.sendError(401); // Need to change the error code
            return;                        
        }        
        
 	try // prepare SQL, extract data , format and return the data
	{
   
            // build the SQL query
            stmt = con.createStatement();
            
            String sql = " SELECT TOP 1000 Id as RegexpId, Name as RegexpName, Expression as Regexp ";            
            sql += " FROM dbo.ChevelleQuery";
            // log(sql);
            
            rs = stmt.executeQuery(sql);            
            int rec_count = 0;
       
            //create string variables for each column
            String val_RegexpId = "";
            String val_RegexpName = "";         
            String val_Regexp = "";
            
            //create string variables for each previous column value 
            String prev_val_RegexpId = "";
            String prev_val_RegexpName = "";         
            String prev_val_Regexp = "";
                        
            StringBuilder text_response = new StringBuilder();
            StringBuilder json_response = new StringBuilder();            
            
            while(rs.next())
            {
                rec_count++;   
                
                val_RegexpId = rs.getString("RegexpId");
                if (rs.wasNull())
                    val_RegexpId = "";
                else
                    val_RegexpId = val_RegexpId.trim();        
                

                val_RegexpName = rs.getString("RegexpName");
                if (rs.wasNull())
                    val_RegexpName = "";
                else
                    val_RegexpName = val_RegexpName.trim();                

                
                val_Regexp = rs.getString("Regexp");
                if (rs.wasNull())
                    val_Regexp = "";
                else
                    val_Regexp = encodeRegexp(val_Regexp.trim());     
                
                
                if (rec_count > 1) //this is second pass - two or more record - only for json format
                {
                    if (format.equals("json"))
                    { 
                        json_response.setLength(0); // clear the buffer
                        
                        json_response.append("{ \"RegexpId\": \"");  json_response.append(prev_val_RegexpId); json_response.append("\"");                        
                        json_response.append(", \"RegexpName\": \"");  json_response.append(prev_val_RegexpName); json_response.append("\"");
                        json_response.append(", \"Regexp\": \"");  json_response.append(prev_val_Regexp); json_response.append("\" }, ");

                        out.println(json_response.toString());
                    }
                    
                    if (format.equals("text"))
                    {
                        text_response.setLength(0);
                        
                        text_response.append(prev_val_RegexpId); text_response.append(" ");                        
                        text_response.append(prev_val_RegexpName); text_response.append(" ");
                        text_response.append(prev_val_Regexp); text_response.append(" ");
                       
                        out.println(text_response.toString());
                    }
                }       
                
                prev_val_RegexpId = val_RegexpId;
                prev_val_RegexpName = val_RegexpName;         
                prev_val_Regexp = val_Regexp;
                
            }
            
                if (rec_count > 0) // at least one record - the last record
                {
                    if (format.equals("json"))
                    { 
                        json_response.setLength(0); // clear the buffer
                        
                        json_response.append("{ \"RegexpId\": \"");  json_response.append(prev_val_RegexpId); json_response.append("\"");                        
                        json_response.append(", \"RegexpName\": \"");  json_response.append(prev_val_RegexpName); json_response.append("\"");
                        json_response.append(", \"Regexp\": \"");  json_response.append(prev_val_Regexp); json_response.append("\" } ");

                        out.println(json_response.toString());
                    }
                    
                    if (format.equals("text"))
                    {
                        text_response.setLength(0);
                        
                        text_response.append(prev_val_RegexpId); text_response.append(" ");                        
                        text_response.append(prev_val_RegexpName); text_response.append(" ");
                        text_response.append(prev_val_Regexp); text_response.append(" ");
                       
                        out.println(text_response.toString());
                    }
                }                
            
            
            out.flush();
            
            if (format.equals("json"))
            {
                out.println("]");
            }
            
            // Clean-up environment
            rs.close();
            rs = null;
            stmt.close();
            stmt = null;
            try {con.close();} //Connection released to the Connection Pool
            catch(Exception e)
            {
                out.print("GetMaxisColRegServlet: Cannot close and release connection to the Pool: " + e.getMessage());
                log("GetMaxisColRegServlet: Cannot close and release connection to the Pool: " + e.getMessage());
            } // If connection pooling, return to connection pool; DO NOT set to null                                               
            
            
        } // the BIG try to prepare SQL, extract data , format and return the data
        catch(SQLException ex)
        {
            log("Error: GetMaxisColRegServlet - SQL exception: " + ex.getMessage());
	    out.println("Error: connecting or executing database command: " + ex.getMessage());            
            response.setStatus(409); // Need to change the error code
            response.sendError(409); // Need to change the error code
            // return;            
        }
        catch (Exception e) 
        {
            log("Error: GetMaxisColReg servlet major error: " + e.getMessage());
	    out.println("Error: GetMaxisColReg servlet major error: " + e.getMessage());            
            response.setStatus(500); // Need to change the error code
            response.sendError(500); // Need to change the error code            
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

    public String encodeRegexp(String regexpression)
    {
        // encode all backslash (escape) characters with the copyright character to avoid loading problems in Angular
        char[] bufferChars = regexpression.toCharArray();
        for (int i = 0; i < bufferChars.length; i++)
        {
            if (bufferChars[i] == (char) 0x5C) // backslash "\"
                bufferChars[i] = (char) 0xA9;  // copywright "©" 
        }
        return String.valueOf(bufferChars);              
    }    
    
    
}
