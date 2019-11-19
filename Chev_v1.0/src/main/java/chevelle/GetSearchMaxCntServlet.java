/**************************************************************************
 * Servlet: GetSearchMaxCntServlet
 * URL: /GetSearchMaxCnt
 * Call method (prefered): GET
 * Parameters:
 *  - gdap_layer: EDGE_BASE or IC
 *  - searchby: the field in the MasterTable to search by: 
 *          - TableName, 
 *          - BusinessName, 
 *          - SourceTableName, 
 *          - Asms, 
 *          - SourcingProject, 
 *          - SubjectArea, 
 *          - SourceDbName, 
 *          - SourceSchemaName, 
 *          - HiveSchema, 
 *          - AutosysJobId 
 *  - searchval: the current value of the searchby field
 *  - format: the output format of the response (json, text)
 * Author: Dan Grebenisan
 * History: - Apr 2018 - Created
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
import org.apache.tomcat.jdbc.pool.DataSource; //test to see if it works in the cloud
//import org.apache.tomcat.jdbc.pool.PoolProperties;
//import javax.sql.DataSource;
// import chevelle.ChevelleEnvironment;
// import chevelle.ChevelleHelper;


public class GetSearchMaxCntServlet extends HttpServlet {

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
        
        // chevelleEnvironment = new ChevelleEnvironment();
        // chevelleHelper = new ChevelleHelper(); 
        
/*        
        try
        {
            initContext = new InitialContext();
            envContext = (Context)initContext.lookup("java:/comp/env");
            
            CHEVELLE_ENV = (String) envContext.lookup("CHEVELLE_ENV");
            log("Chevelle environment look-up success: " + CHEVELLE_ENV);
            
            ServletContext context = getServletContext();
            ds = (org.apache.tomcat.jdbc.pool.DataSource) context.getAttribute("chevelle_connection_pool");
            log("Data Source successfully retrieved from the servlet context!");
        }
        catch (NamingException e)
        {
            log("Cannot retrieve the CHEVELLE_ENV or the conection pooled DataSource: " + e.getMessage());
        }
*/
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
        Boolean searchfuzz = false;
        
        PrintWriter out = null;
        
        String gdap_layer = request.getParameter("gdap_layer");
        if(gdap_layer == null)
            gdap_layer = ""; // gdap_layer       
        
        String searchby = request.getParameter("searchby");
        if(searchby == null)
            searchby = ""; // searchby               
        
        String searchval = request.getParameter("searchval");
        if(searchval == null)
        {
            searchval = ""; // searchval
            searchfuzz = false;
        }
        else
        {
            if (searchval.contains("*"))
            {
                //replace the * wildcards with "%"
                char[] searchChars = searchval.toCharArray();
                for (int i = 0; i < searchChars.length; i++)
                {
                    if (searchChars[i] == '*')
                        searchChars[i] = '%';   
                }
                searchval = String.valueOf(searchChars);
                searchfuzz = true;
            }
            else
                searchfuzz = false;
        }
        // log(searchval);        
       
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
            log("Error: GetSearchMaxCntServlet - Cannot open a stream for the client: " + ex.getMessage());
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
        
           
        // connect to DB, extract data , format and return the data
 	try 
	{
            try
            {
                con = ds.getConnection();
                // log("connection suuceeesfully retrieved from Connection Pool");
            }
            catch(Exception e)
            {
                log("cannot get a pooled connection from the DataSource");
            }
	    
            // build the SQL query
            stmt = con.createStatement();
            stmt.setQueryTimeout(120);
            
            String sql = "SELECT COUNT(*) AS total_cnt ";            
            sql += " FROM dbo.MasterTable (nolock)";
            sql += " WHERE GdapArchLayer = '" + gdap_layer + "' ";

            if (searchby.equals("TableName"))
            {
                if (searchfuzz)
                {
                    sql = sql + " AND TableName like '" + searchval + "'";     
                }
                else
                {
                    sql = sql + " AND TableName = '" + searchval + "'";     
                }                                                
            }      
            else if (searchby.equals("BusinessName"))
            {
                if (searchfuzz)
                {
                    sql = sql + " AND BusinessName like '" + searchval + "'";
                }
                else
                {
                    sql = sql + " AND BusinessName = '" + searchval + "'";
                }
            }    
            else if (searchby.equals("SourceTableName"))
            {
                if (searchfuzz)
                {
                    sql = sql + " AND SourceTableName like '" + searchval + "'";
                }
                else
                {
                    sql = sql + " AND SourceTableName = '" + searchval + "'";
                }
            }   
            else if (searchby.equals("Asms"))
            {
                if (searchfuzz)
                {
                    sql = sql + " AND Asms like '" + searchval + "'";
                }
                else
                {
                    sql = sql + " AND Asms = '" + searchval + "'";
                }                
            }                        
            else if (searchby.equals("SourcingProject"))
            {
                if (searchfuzz)
                {
                    sql = sql + " AND SourcingProject like '" + searchval + "'";
                }
                else
                {
                    sql = sql + " AND SourcingProject = '" + searchval + "'";
                }
            }
            else if (searchby.equals("SubjectArea"))
            {
                if (searchfuzz)
                {
                    sql = sql + " AND SubjectArea like '" + searchval + "'";
                }
                else
                {
                    sql = sql + " AND SubjectArea = '" + searchval + "'";
                }                                
            }
            else if (searchby.equals("SourceDbName"))
            {
                if (searchfuzz)
                {
                    sql = sql + " AND SourceDbName like '" + searchval + "'";     
                }
                else
                {
                    sql = sql + " AND SourceDbName = '" + searchval + "'";     
                }                                
            }      
            else if (searchby.equals("SourceSchemaName"))
            {
                if (searchfuzz)
                {
                    sql = sql + " AND SourceSchemaName like '" + searchval + "'";     
                }
                else
                {
                    sql = sql + " AND SourceSchemaName = '" + searchval + "'";     
                }                                
            }     
            else if (searchby.equals("HiveSchema"))
            {
                if (searchfuzz)
                {
                    sql = sql + " AND HiveSchema like '" + searchval + "'";
                }
                else
                {
                    sql = sql + " AND HiveSchema = '" + searchval + "'";
                }                
            }            
            else if (searchby.equals("AutosysJobId"))
            {
                if (searchfuzz)
                {
                    sql = sql + " AND AutosysJobId like '" + searchval + "'";
                }
                else
                {
                    sql = sql + " AND AutosysJobId = '" + searchval + "'";
                }
            }
            else
            {
                ;
            }
            
            rs = stmt.executeQuery(sql);                        
            
            int rec_count = 0;
            String total_cnt = "0";
            
            while(rs.next())
            {
                rec_count++;                
            
                total_cnt = rs.getString("total_cnt");
                if (rs.wasNull())
                    total_cnt = "0";
            }
            
            if (format.equals("json"))
            {             
                out.println("{ \"total_cnt\": \"" + total_cnt  + "\" }");
            }
 
            if (format.equals("text"))
            {   
                out.println("total_cnt: " + total_cnt);
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
                out.print("GetSearchMaxCntServlet: Cannot close and release connection to the Connection Pool: " + e.getMessage());
                log("GetSearchMaxCntServlet: Cannot close and release connection to the DB Connection Pool: " + e.getMessage());
            } // If connection pooling, return to connection pool; DO NOT set to null                       
                        
            
        } // the big try to get a DB connection, execute and retrieve from DB
        catch(SQLException ex)
        {
            log("Error: GetSearchMaxCntServlet - SQL exception: " + ex.getMessage());
	    out.println("Error: connecting or executing database command: " + ex.getMessage());            
            response.setStatus(409); // Need to change the error code
            // response.sendError(409); // Need to change the error code
            return;            
        }
        catch (Exception e) 
        {
            log("Error: GetSearchMaxCnt servlet major error: " + e.getMessage());
	    out.println("Error: GetMaxisTables servlet major error: " + e.getMessage());            
            response.setStatus(500); // Need to change the error code
            // response.sendError(500); // Need to change the error code            
            return;
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
