/**************************************************************************
 * Servlet: GetMaxisTablesServlet
 * URL: /GetMaxisTables
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
 *  - offset: the starting offset of the pagination
 *  - page_size: the number of records in the current page
 *  - format: the output format of the response (json, text)
 * Author: Dan Grebenisan
 * History: - Mar 2018 - Created
 *          - Mar 26, 2018 - Changed the MasterTable_Tmp to main table
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
import javax.servlet.ServletContext;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
//import javax.sql.DataSource;
import org.apache.tomcat.jdbc.pool.DataSource; //test to see if it works in the cloud

/**
 *
 * @author BZY91Q
 */
public class GetMaxisTablesServlet extends HttpServlet {

    
    // InitialContext initContext = null;
    // Context envContext = null;
    // DataSource ds = null;
    // String CHEVELLE_ENV = "";
    
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
 /*       
        try
        {
            log("Initializing the DB Connection Pooling for the GetMaxisCfg REST servlet");
            initContext = new InitialContext();
            envContext = (Context)initContext.lookup("java:/comp/env");
            log("Initial and Environment context retrieval success!");
            ds = (DataSource)envContext.lookup("jdbc/maxis_dbcp");
            log("Data source 'maxis_dbcp' look-up success");
            
            CHEVELLE_ENV = (String) envContext.lookup("CHEVELLE_ENV");
            log("Chevelle environment look-up success: " + CHEVELLE_ENV);
            
            //DataSource ds = (DataSource)initContext.lookup("java:/comp/env/jdbc/jdbc/icl_dbcp");
            
            // This code doesn't work unless all parameters are present in the context.xml of the app
            // ds.setUrl("jdbc:sqlserver://epgidvwgds1139.epga.nam.domain.com:1433;DatabaseName=Maxis");
            // ds.setUsername("chevelle");
            // ds.setPassword("fG68YVJWveki80oB");
            
            
        }
        catch (NamingException e)
        {
            log("Cannot initiale the DB COnnection Pooling for the GetMaxisCfg REST servlet, or look-up the data source!");
            log("Naming exception error: " + e.getMessage());            
        }
*/
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
        
        //get the starting offset
        String offset = request.getParameter("offset");
        if(offset == null)
            offset = "0"; // start at zero 

        //get page size
        String page_size = request.getParameter("page_size");
        if(page_size == null)
            page_size = "1000"; // 1000 records per page unless specified otherwise         
        
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
            log("Error: GetMaxisTablesServlet - Cannot open a stream for the client: " + ex.getMessage());
            response.setStatus(500);
            response.sendError(500);
            out = null;
            return;
        }        

/* just for testing        
        out.println("gdap_layer: " + gdap_layer);
        out.println("searchby: " + searchby);
        out.println("searchval: " + searchval);
        out.println("format: " + format);
*/
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
        
        
        // connect to DB, extract data , format and return the data
 	try 
	{
            
            con = ds.getConnection();
            // log("connection suuceeesfully retrieved from Connection Pool");
	    
            // build the SQL query
            stmt = con.createStatement();
            stmt.setQueryTimeout(120);
            
            String sql = "SELECT Id, TableName, BusinessName, SourceTableName, Description, Asms, GdapArchLayer, SourcingProject, SubjectArea, SourceDbName, SourceSchemaName, HiveSchema, Location, AutosysJobId, CreatedBy, CreatedDate, UpdatedBy, UpdatedDate, ImportId, TotalRecords, TotalRecordsUpdatedDate ";            
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
            
            sql = sql + " ORDER BY Id ";
            sql = sql + " OFFSET " + offset + " ROWS ";
            sql = sql + " FETCH NEXT " + page_size + " ROWS ONLY";
            // log(sql);
            
            rs = stmt.executeQuery(sql);            
            
            int rec_count = 0;
            
            //create string variables for each column
            String val_Id = "";
            String val_TableName = "";
            String val_BusinessName = "";
            String val_SourceTableName = ""; 
            String val_Description = ""; 
            String val_Asms = ""; 
            String val_GdapArchLayer = ""; 
            String val_SourcingProject = ""; 
            String val_SubjectArea = ""; 
            String val_SourceDbName = ""; 
            String val_SourceSchemaName = ""; 
            String val_HiveSchema = ""; 
            String val_Location = ""; 
            String val_AutosysJobId = ""; 
            String val_CreatedBy = ""; 
            String val_CreatedDate = ""; 
            String val_UpdatedBy = ""; 
            String val_UpdatedDate = ""; 
            String val_ImportId = ""; 
            String val_TotalRecords = "";
            String val_TotalRecordsUpdatedDate = "";
            
            //create string variables for each previous column value
            String prev_val_Id = "";
            String prev_val_TableName = "";
            String prev_val_BusinessName = "";
            String prev_val_SourceTableName = ""; 
            String prev_val_Description = ""; 
            String prev_val_Asms = ""; 
            String prev_val_GdapArchLayer = ""; 
            String prev_val_SourcingProject = ""; 
            String prev_val_SubjectArea = ""; 
            String prev_val_SourceDbName = ""; 
            String prev_val_SourceSchemaName = ""; 
            String prev_val_HiveSchema = ""; 
            String prev_val_Location = ""; 
            String prev_val_AutosysJobId = ""; 
            String prev_val_CreatedBy = ""; 
            String prev_val_CreatedDate = ""; 
            String prev_val_UpdatedBy = ""; 
            String prev_val_UpdatedDate = ""; 
            String prev_val_ImportId = ""; 
            String prev_val_TotalRecords = "";
            String prev_val_TotalRecordsUpdatedDate = "";            
            
            StringBuilder text_response = new StringBuilder();
            StringBuilder json_response = new StringBuilder();
            
            while(rs.next())
            {
                rec_count++;                
            
                val_Id = rs.getString("Id");
                if (rs.wasNull())
                    val_Id = "";
                else
                    val_Id = val_Id.trim();
                
                
                val_TableName = rs.getString("TableName");
                if (rs.wasNull())
                    val_TableName = "";
                else
                    val_TableName = val_TableName.trim();
                
                
                val_BusinessName = rs.getString("BusinessName");
                if (rs.wasNull())
                    val_BusinessName = "";
                else
                    val_BusinessName = val_BusinessName.trim();
                
                
                val_SourceTableName = rs.getString("SourceTableName"); 
                if (rs.wasNull())
                    val_SourceTableName = "";
                else
                    val_SourceTableName = val_SourceTableName.trim();
                
                
                val_Description = rs.getString("Description"); 
                if (rs.wasNull())
                    val_Description = "";
                else
                    val_Description = val_Description.trim();
                
                
                val_Asms = rs.getString("Asms"); 
                if (rs.wasNull())
                    val_Asms = "";
                else
                    val_Asms = val_Asms.trim();
                
                
                val_GdapArchLayer = rs.getString("GdapArchLayer"); 
                if (rs.wasNull())
                    val_GdapArchLayer = "";
                else
                    val_GdapArchLayer = val_GdapArchLayer.trim();
                
                
                val_SourcingProject = rs.getString("SourcingProject"); 
                if (rs.wasNull())
                    val_SourcingProject = "";
                else
                    val_SourcingProject = val_SourcingProject.trim();
                
                
                val_SubjectArea = rs.getString("SubjectArea"); 
                if (rs.wasNull())
                    val_SubjectArea = "";
                else
                    val_SubjectArea = val_SubjectArea.trim();
                
                
                val_SourceDbName = rs.getString("SourceDbName"); 
                if (rs.wasNull())
                    val_SourceDbName = "";
                else
                    val_SourceDbName = val_SourceDbName.trim();
                
                
                val_SourceSchemaName = rs.getString("SourceSchemaName"); 
                if (rs.wasNull())
                    val_SourceSchemaName = "";
                else
                    val_SourceSchemaName = val_SourceSchemaName.trim();
                
                
                val_HiveSchema = rs.getString("HiveSchema"); 
                if (rs.wasNull())
                    val_HiveSchema = "";
                else
                    val_HiveSchema = val_HiveSchema.trim();
                
                
                val_Location = rs.getString("Location"); 
                if (rs.wasNull())
                    val_Location = "";
                else
                    val_Location = val_Location.trim(); 
                
                
                val_AutosysJobId = rs.getString("AutosysJobId"); 
                if (rs.wasNull())
                    val_AutosysJobId = "";
                else
                    val_AutosysJobId = val_AutosysJobId.trim(); 
                
                
                val_CreatedBy = rs.getString("CreatedBy"); 
                if (rs.wasNull())
                    val_CreatedBy = "";
                else
                    val_CreatedBy = val_CreatedBy.trim();
                
                
                val_CreatedDate = rs.getString("CreatedDate"); 
                if (rs.wasNull())
                    val_CreatedDate = "";
                else
                    val_CreatedDate = val_CreatedDate.trim();
                
                
                val_UpdatedBy = rs.getString("UpdatedBy"); 
                if (rs.wasNull())
                    val_UpdatedBy = "";
                else
                    val_UpdatedBy = val_UpdatedBy.trim();
                
                
                val_UpdatedDate = rs.getString("UpdatedDate"); 
                if (rs.wasNull())
                    val_UpdatedDate = "";
                else
                    val_UpdatedDate = val_UpdatedDate.trim();
                
                
                val_ImportId = rs.getString("ImportId"); 
                if (rs.wasNull())
                    val_ImportId = "";
                else
                    val_ImportId = val_ImportId.trim();
                
                
                val_TotalRecords = rs.getString("TotalRecords");
                if (rs.wasNull())
                    val_TotalRecords = "";
                else
                    val_TotalRecords = val_TotalRecords.trim();
                
                
                val_TotalRecordsUpdatedDate = rs.getString("TotalRecordsUpdatedDate");                
                if (rs.wasNull())
                    val_TotalRecordsUpdatedDate = "";
                else
                    val_TotalRecordsUpdatedDate = val_TotalRecordsUpdatedDate.trim();
                
               // out.println("Id: " + val_Id + " TableName: " + val_TableName);
                
                if (rec_count > 1) //this is second pass - two or more record - only for json format
                {
                    if (format.equals("json"))
                    { 
                        json_response.setLength(0); // clear the buffer
                        json_response.append("{ \"Id\": \""); json_response.append(prev_val_Id); json_response.append("\"");
                        json_response.append(", \"TableName\": \"");  json_response.append(prev_val_TableName); json_response.append("\"");
                        json_response.append(", \"BusinessName\": \"");  json_response.append(prev_val_BusinessName); json_response.append("\"");
                        json_response.append(", \"SourceTableName\": \"");  json_response.append(prev_val_SourceTableName); json_response.append("\"");
                        json_response.append(", \"Description\": \"");  json_response.append(prev_val_Description); json_response.append("\"");
                        json_response.append(", \"Asms\": \"");  json_response.append(prev_val_Asms); json_response.append("\"");
                        json_response.append(", \"GdapArchLayer\": \"");  json_response.append(prev_val_GdapArchLayer); json_response.append("\"");
                        json_response.append(", \"SourcingProject\": \"");  json_response.append(prev_val_SourcingProject); json_response.append("\"");
                        json_response.append(", \"SubjectArea\": \"");  json_response.append(prev_val_SubjectArea); json_response.append("\"");
                        json_response.append(", \"SourceDbName\": \"");  json_response.append(prev_val_SourceDbName); json_response.append("\"");
                        json_response.append(", \"SourceSchemaName\": \"");  json_response.append(prev_val_SourceSchemaName); json_response.append("\"");
                        json_response.append(", \"HiveSchema\": \"");  json_response.append(prev_val_HiveSchema); json_response.append("\"");
                        json_response.append(", \"Location\": \"");  json_response.append(prev_val_Location); json_response.append("\"");
                        json_response.append(", \"AutosysJobId\": \"");  json_response.append(prev_val_AutosysJobId); json_response.append("\"");
                        json_response.append(", \"CreatedBy\": \"");  json_response.append(prev_val_CreatedBy); json_response.append("\"");
                        json_response.append(", \"CreatedDate\": \"");  json_response.append(prev_val_CreatedDate); json_response.append("\"");
                        json_response.append(", \"UpdatedBy\": \"");  json_response.append(prev_val_UpdatedBy); json_response.append("\"");
                        json_response.append(", \"UpdatedDate\": \"");  json_response.append(prev_val_UpdatedDate); json_response.append("\"");
                        json_response.append(", \"ImportId\": \"");  json_response.append(prev_val_ImportId); json_response.append("\"");
                        json_response.append(", \"TotalRecords\": \"");  json_response.append(prev_val_TotalRecords); json_response.append("\"");
                        json_response.append(", \"TotalRecordsUpdatedDate\": \"");  json_response.append(prev_val_TotalRecordsUpdatedDate); json_response.append("\" }, ");
                        
                        out.println(json_response.toString());
                    }
                    
                    if (format.equals("text"))
                    {
                        text_response.setLength(0);
                        text_response.append(prev_val_Id); text_response.append(" ");
                        text_response.append(prev_val_TableName); text_response.append(" ");
                        text_response.append(prev_val_BusinessName); text_response.append(" ");
                        text_response.append(prev_val_SourceTableName); text_response.append(" ");
                        text_response.append(prev_val_Description); text_response.append(" ");
                        text_response.append(prev_val_Asms); text_response.append(" ");
                        text_response.append(prev_val_GdapArchLayer); text_response.append(" ");
                        text_response.append(prev_val_SourcingProject); text_response.append(" ");
                        text_response.append(prev_val_SubjectArea); text_response.append(" ");
                        text_response.append(prev_val_SourceDbName); text_response.append(" ");
                        text_response.append(prev_val_SourceSchemaName); text_response.append(" ");
                        text_response.append(prev_val_HiveSchema); text_response.append(" ");
                        text_response.append(prev_val_Location); text_response.append(" ");
                        text_response.append(prev_val_AutosysJobId); text_response.append(" ");
                        text_response.append(prev_val_CreatedBy); text_response.append(" ");
                        text_response.append(prev_val_CreatedDate); text_response.append(" ");
                        text_response.append(prev_val_UpdatedBy); text_response.append(" ");
                        text_response.append(prev_val_UpdatedDate); text_response.append(" ");
                        text_response.append(prev_val_ImportId); text_response.append(" ");
                        text_response.append(prev_val_TotalRecords); text_response.append(" ");
                        text_response.append(prev_val_TotalRecordsUpdatedDate); 
                        
                        out.println(text_response.toString());
                    }
                } // the second pass
                
                prev_val_Id = val_Id;
                prev_val_TableName = val_TableName;
                prev_val_BusinessName = val_BusinessName;
                prev_val_SourceTableName = val_SourceTableName; 
                prev_val_Description = val_Description; 
                prev_val_Asms = val_Asms; 
                prev_val_GdapArchLayer = val_GdapArchLayer; 
                prev_val_SourcingProject = val_SourcingProject; 
                prev_val_SubjectArea = val_SubjectArea; 
                prev_val_SourceDbName = val_SourceDbName; 
                prev_val_SourceSchemaName = val_SourceSchemaName; 
                prev_val_HiveSchema = val_HiveSchema; 
                prev_val_Location = val_Location; 
                prev_val_AutosysJobId = val_AutosysJobId; 
                prev_val_CreatedBy = val_CreatedBy; 
                prev_val_CreatedDate = val_CreatedDate; 
                prev_val_UpdatedBy = val_UpdatedBy; 
                prev_val_UpdatedDate = val_UpdatedDate; 
                prev_val_ImportId = val_ImportId; 
                prev_val_TotalRecords = val_TotalRecords;
                prev_val_TotalRecordsUpdatedDate = val_TotalRecordsUpdatedDate;                      
                        
            }
            
            if (rec_count > 0) //at least one record
            {
                if (format.equals("json"))
                { 
                    json_response.setLength(0); // clear the buffer
                    json_response.append("{ \"Id\": \""); json_response.append(prev_val_Id); json_response.append("\"");
                    json_response.append(", \"TableName\": \"");  json_response.append(prev_val_TableName); json_response.append("\"");
                    json_response.append(", \"BusinessName\": \"");  json_response.append(prev_val_BusinessName); json_response.append("\"");
                    json_response.append(", \"SourceTableName\": \"");  json_response.append(prev_val_SourceTableName); json_response.append("\"");
                    json_response.append(", \"Description\": \"");  json_response.append(prev_val_Description); json_response.append("\"");
                    json_response.append(", \"Asms\": \"");  json_response.append(prev_val_Asms); json_response.append("\"");
                    json_response.append(", \"GdapArchLayer\": \"");  json_response.append(prev_val_GdapArchLayer); json_response.append("\"");
                    json_response.append(", \"SourcingProject\": \"");  json_response.append(prev_val_SourcingProject); json_response.append("\"");
                    json_response.append(", \"SubjectArea\": \"");  json_response.append(prev_val_SubjectArea); json_response.append("\"");
                    json_response.append(", \"SourceDbName\": \"");  json_response.append(prev_val_SourceDbName); json_response.append("\"");
                    json_response.append(", \"SourceSchemaName\": \"");  json_response.append(prev_val_SourceSchemaName); json_response.append("\"");
                    json_response.append(", \"HiveSchema\": \"");  json_response.append(prev_val_HiveSchema); json_response.append("\"");
                    json_response.append(", \"Location\": \"");  json_response.append(prev_val_Location); json_response.append("\"");
                    json_response.append(", \"AutosysJobId\": \"");  json_response.append(prev_val_AutosysJobId); json_response.append("\"");
                    json_response.append(", \"CreatedBy\": \"");  json_response.append(prev_val_CreatedBy); json_response.append("\"");
                    json_response.append(", \"CreatedDate\": \"");  json_response.append(prev_val_CreatedDate); json_response.append("\"");
                    json_response.append(", \"UpdatedBy\": \"");  json_response.append(prev_val_UpdatedBy); json_response.append("\"");
                    json_response.append(", \"UpdatedDate\": \"");  json_response.append(prev_val_UpdatedDate); json_response.append("\"");
                    json_response.append(", \"ImportId\": \"");  json_response.append(prev_val_ImportId); json_response.append("\"");
                    json_response.append(", \"TotalRecords\": \"");  json_response.append(prev_val_TotalRecords); json_response.append("\"");
                    json_response.append(", \"TotalRecordsUpdatedDate\": \"");  json_response.append(prev_val_TotalRecordsUpdatedDate); json_response.append("\" } ");

                    out.println(json_response.toString());
                }

                if (format.equals("text"))
                {
                    text_response.setLength(0);
                    text_response.append(prev_val_Id); text_response.append(" ");
                    text_response.append(prev_val_TableName); text_response.append(" ");
                    text_response.append(prev_val_BusinessName); text_response.append(" ");
                    text_response.append(prev_val_SourceTableName); text_response.append(" ");
                    text_response.append(prev_val_Description); text_response.append(" ");
                    text_response.append(prev_val_Asms); text_response.append(" ");
                    text_response.append(prev_val_GdapArchLayer); text_response.append(" ");
                    text_response.append(prev_val_SourcingProject); text_response.append(" ");
                    text_response.append(prev_val_SubjectArea); text_response.append(" ");
                    text_response.append(prev_val_SourceDbName); text_response.append(" ");
                    text_response.append(prev_val_SourceSchemaName); text_response.append(" ");
                    text_response.append(prev_val_HiveSchema); text_response.append(" ");
                    text_response.append(prev_val_Location); text_response.append(" ");
                    text_response.append(prev_val_AutosysJobId); text_response.append(" ");
                    text_response.append(prev_val_CreatedBy); text_response.append(" ");
                    text_response.append(prev_val_CreatedDate); text_response.append(" ");
                    text_response.append(prev_val_UpdatedBy); text_response.append(" ");
                    text_response.append(prev_val_UpdatedDate); text_response.append(" ");
                    text_response.append(prev_val_ImportId); text_response.append(" ");
                    text_response.append(prev_val_TotalRecords); text_response.append(" ");
                    text_response.append(prev_val_TotalRecordsUpdatedDate); 

                    out.println(text_response.toString());
                }
            }    
           
            if (format.equals("json"))
            {
                out.println("]");
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
                out.print("GetMaxisTablesServlet: Cannot close and release connection to the Connection Pool: " + e.getMessage());
                log("GetMaxisTablesServlet: Cannot close and release connection to the DB Connection Pool: " + e.getMessage());
            } // If connection pooling, return to connection pool; DO NOT set to null                       
            
        } // the big try to get a DB connection, execute and retrieve from DB
        catch(SQLException ex)
        {
            log("Error: GetMaxisTablesServlet - SQL exception: " + ex.getMessage());
	    out.println("Error: connecting or executing database command: " + ex.getMessage());            
            response.setStatus(409); // Need to change the error code
            // response.sendError(409); // Need to change the error code
            return;            
        }
        catch (Exception e) 
        {
            log("Error: GetMaxisTables servlet major error: " + e.getMessage());
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
