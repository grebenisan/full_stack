/**************************************************************************
 * Servlet: GetMaxisColumnsServlet
 * URL: /GetMaxisColumns
 * Call method (prefered): GET
 * Parameters:
 *  - TableId: the ID of the table
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
public class GetMaxisColumnsServlet extends HttpServlet {

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
        
        String str_TableId = request.getParameter("TableId");
        if(str_TableId == null)
            str_TableId = ""; // the TableID search       
        
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
            log("Error: GetMaxisColumnsServlet - Cannot open the out stream: " + ex.getMessage());
            response.setStatus(500);
            response.sendError(500);
            out = null;
            return;
        }        
/*        
        out.println("TableId: " + TableId);
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
       
        
        // get connection from DB
 	try 
	{
            con = ds.getConnection();

        }
        catch(Exception ex)
        {
            log("Error: GetMaxisColumnsServlet cannot get connection: " + ex.getMessage());
	    out.println("Error: GetMaxisColumnsServlet cannot get connection: " + ex.getMessage());            
            response.setStatus(401); // Need to change the error code
            response.sendError(401); // Need to change the error code
            return;                        
        }
        
 	try // prepare SQL, extract data , format and return the data
	{
   
            // build the SQL query
            stmt = con.createStatement();
            stmt.setQueryTimeout(120);
            
            String sql = "SELECT TOP 1000 Id, ColumnName, BusinessName, SourceColumnName, Description, DataType, DataLength, IsNullable, IsPrimaryKey, IsForeignKey, IsMultibyte, IsCertified, DataClassification, UniqueRecords, NullRecords, DefaultRecords, MinValue, MaxValue, MeanValue, StDev, MinLength, MaxLength, AvgLength, StatsUpdatedDate ";            
            sql += " FROM dbo.MasterColumn ";
            sql += " WHERE TableId = '" + str_TableId + "'";
            
            rs = stmt.executeQuery(sql);            
            
            int rec_count = 0;            
            
            //create string variables for each column
            String val_Id = "";
            String val_ColumnName = ""; 
            String val_BusinessName = ""; 
            String val_SourceColumnName = "";            
            String val_Description = ""; 
            String val_DataType = ""; 
            String val_DataLength = ""; 
            String val_IsNullable = ""; 
            String val_IsPrimaryKey = ""; 
            String val_IsForeignKey= ""; 
            String val_IsMultibyte = ""; 
            String val_IsCertified = ""; 
            String val_DataClassification = ""; 
            String val_UniqueRecords = ""; 
            String val_NullRecords = ""; 
            String val_DefaultRecords = ""; 
            String val_MinValue = "";
            String val_MaxValue = "";
            String val_MeanValue = "";
            String val_StDev = "";
            String val_MinLength = "";
            String val_MaxLength = "";
            String val_AvgLength = "";
            String val_StatsUpdatedDate = "";
            
            //create string variables for each previous column value
            String prev_val_Id = "";
            String prev_val_ColumnName = ""; 
            String prev_val_BusinessName = ""; 
            String prev_val_SourceColumnName = "";            
            String prev_val_Description = ""; 
            String prev_val_DataType = ""; 
            String prev_val_DataLength = ""; 
            String prev_val_IsNullable = ""; 
            String prev_val_IsPrimaryKey = ""; 
            String prev_val_IsForeignKey= ""; 
            String prev_val_IsMultibyte = ""; 
            String prev_val_IsCertified = ""; 
            String prev_val_DataClassification = ""; 
            String prev_val_UniqueRecords = ""; 
            String prev_val_NullRecords = ""; 
            String prev_val_DefaultRecords = ""; 
            String prev_val_MinValue = "";
            String prev_val_MaxValue = "";
            String prev_val_MeanValue = "";
            String prev_val_StDev = "";
            String prev_val_MinLength = "";
            String prev_val_MaxLength = "";
            String prev_val_AvgLength = "";
            String prev_val_StatsUpdatedDate = "";            
            
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
                
                
                val_ColumnName = rs.getString("ColumnName");
                if (rs.wasNull())
                    val_ColumnName = "";
                else
                    val_ColumnName = val_ColumnName.trim();
                
                
                val_BusinessName = rs.getString("BusinessName");
                if (rs.wasNull())
                    val_BusinessName = "";
                else
                    val_BusinessName = val_BusinessName.trim();

                
                val_SourceColumnName = rs.getString("SourceColumnName");
                if (rs.wasNull())
                    val_SourceColumnName = "";    
                else
                    val_SourceColumnName = val_SourceColumnName.trim();
                
                
                val_Description = rs.getString("Description");
                if (rs.wasNull())
                    val_Description = "";   
                else
                    val_Description = val_Description.trim();
                
                
                val_DataType = rs.getString("DataType");
                if (rs.wasNull())
                    val_DataType = "";   
                else
                    val_DataType = val_DataType.trim();
                

                val_DataLength = rs.getString("DataLength");
                if (rs.wasNull())
                    val_DataLength = "";  
                else
                    val_DataLength = val_DataLength.trim();
                

                val_IsNullable = rs.getString("IsNullable");
                if (rs.wasNull())
                    val_IsNullable = "";
                else
                {
                    if(val_IsNullable.equals("1"))
                        val_IsNullable = "true";
                    else
                        val_IsNullable = "false";
                }
                
                
                val_IsPrimaryKey = rs.getString("IsPrimaryKey");
                if (rs.wasNull())
                    val_IsPrimaryKey = "";   
                else
                {
                    if (val_IsPrimaryKey.equals("1"))
                        val_IsPrimaryKey = "true";
                    else
                        val_IsPrimaryKey = "false";
                }
                

                val_IsForeignKey = rs.getString("IsForeignKey");
                if (rs.wasNull())
                    val_IsForeignKey = "";  
                else
                {
                    if (val_IsForeignKey.equals("1"))
                        val_IsForeignKey = "true";
                    else
                        val_IsForeignKey = "false";
                }
                
                
                val_IsMultibyte = rs.getString("IsMultibyte");
                if (rs.wasNull())
                    val_IsMultibyte = "";  
                else
                {
                    if (val_IsMultibyte.equals("1"))
                        val_IsMultibyte = "true";
                    else
                        val_IsMultibyte = "false";
                }
                

                val_IsCertified = rs.getString("IsCertified");
                if (rs.wasNull())
                    val_IsCertified = "";  
                else
                {
                    if (val_IsCertified.equals("1"))
                        val_IsCertified = "true";
                    else
                        val_IsCertified = "false";
                }
                
                
                val_DataClassification = rs.getString("DataClassification");
                if (rs.wasNull())
                    val_DataClassification = "";    
                else
                    val_DataClassification = val_DataClassification.trim();
                
 
                val_UniqueRecords = rs.getString("UniqueRecords");
                if (rs.wasNull())
                    val_UniqueRecords = "";  
                else
                    val_UniqueRecords = val_UniqueRecords.trim();
                

                val_NullRecords = rs.getString("NullRecords");
                if (rs.wasNull())
                    val_NullRecords = "";  
                else
                    val_NullRecords = val_NullRecords.trim();
                
                
                val_DefaultRecords = rs.getString("DefaultRecords");
                if (rs.wasNull())
                    val_DefaultRecords = "";    
                else
                    val_DefaultRecords = val_DefaultRecords.trim();
                
                
                val_MinValue = rs.getString("MinValue");
                if (rs.wasNull())
                    val_MinValue = "";   
                else
                    val_MinValue = val_MinValue.trim();
                
                
                val_MaxValue = rs.getString("MaxValue");
                if (rs.wasNull())
                    val_MaxValue = "";   
                else
                    val_MaxValue = val_MaxValue.trim();
                
                
                val_MeanValue = rs.getString("MeanValue");
                if (rs.wasNull())
                    val_MeanValue = "";  
                else
                    val_MeanValue = val_MeanValue.trim();
                

                val_StDev = rs.getString("StDev");
                if (rs.wasNull())
                    val_StDev = "";   
                else
                    val_StDev = val_StDev.trim();
                

                val_MinLength = rs.getString("MinLength");
                if (rs.wasNull())
                    val_MinLength = "";  
                else
                    val_MinLength = val_MinLength.trim();
                

                val_MaxLength = rs.getString("MaxLength");
                if (rs.wasNull())
                    val_MaxLength = ""; 
                else
                    val_MaxLength = val_MaxLength.trim();
                
                
                val_AvgLength = rs.getString("AvgLength");
                if (rs.wasNull())
                    val_AvgLength = ""; 
                else
                    val_AvgLength = val_AvgLength.trim();
                

                val_StatsUpdatedDate = rs.getString("StatsUpdatedDate");
                if (rs.wasNull())
                    val_StatsUpdatedDate = "";  
                else
                    val_StatsUpdatedDate = val_StatsUpdatedDate.trim();

                
                // out.println("Id: " + val_Id + " ColumnName: " + val_ColumnName); 
                
                if (rec_count > 1) //this is second pass - two or more record - only for json format
                {
                    if (format.equals("json"))
                    { 
                        json_response.setLength(0); // clear the buffer
                        
                        json_response.append("{ \"Id\": \""); json_response.append(prev_val_Id); json_response.append("\"");
                        json_response.append(", \"ColumnName\": \"");  json_response.append(prev_val_ColumnName); json_response.append("\"");
                        json_response.append(", \"BusinessName\": \"");  json_response.append(prev_val_BusinessName); json_response.append("\"");
                        json_response.append(", \"SourceColumnName\": \"");  json_response.append(prev_val_SourceColumnName); json_response.append("\"");
                        json_response.append(", \"Description\": \"");  json_response.append(prev_val_Description); json_response.append("\"");
                        json_response.append(", \"DataType\": \"");  json_response.append(prev_val_DataType); json_response.append("\"");
                        json_response.append(", \"DataLength\": \"");  json_response.append(prev_val_DataLength); json_response.append("\"");
                        json_response.append(", \"IsNullable\": \"");  json_response.append(prev_val_IsNullable); json_response.append("\"");
                        json_response.append(", \"IsPrimaryKey\": \"");  json_response.append(prev_val_IsPrimaryKey); json_response.append("\"");
                        json_response.append(", \"IsForeignKey\": \"");  json_response.append(prev_val_IsForeignKey); json_response.append("\"");
                        json_response.append(", \"IsMultibyte\": \"");  json_response.append(prev_val_IsMultibyte); json_response.append("\"");
                        json_response.append(", \"IsCertified\": \"");  json_response.append(prev_val_IsCertified); json_response.append("\"");
                        json_response.append(", \"DataClassification\": \"");  json_response.append(prev_val_DataClassification); json_response.append("\"");
                        json_response.append(", \"UniqueRecords\": \"");  json_response.append(prev_val_UniqueRecords); json_response.append("\"");
                        json_response.append(", \"NullRecords\": \"");  json_response.append(prev_val_NullRecords); json_response.append("\"");
                        json_response.append(", \"DefaultRecords\": \"");  json_response.append(prev_val_DefaultRecords); json_response.append("\"");
                        json_response.append(", \"MinValue\": \"");  json_response.append(prev_val_MinValue); json_response.append("\"");
                        json_response.append(", \"MaxValue\": \"");  json_response.append(prev_val_MaxValue); json_response.append("\"");
                        json_response.append(", \"MeanValue\": \"");  json_response.append(prev_val_MeanValue); json_response.append("\"");
                        json_response.append(", \"StDev\": \"");  json_response.append(prev_val_StDev); json_response.append("\"");
                        json_response.append(", \"MinLength\": \"");  json_response.append(prev_val_MinLength); json_response.append("\"");
                        json_response.append(", \"MaxLength\": \"");  json_response.append(prev_val_MaxLength); json_response.append("\"");
                        json_response.append(", \"AvgLength\": \"");  json_response.append(prev_val_AvgLength); json_response.append("\"");
                        json_response.append(", \"StatsUpdatedDate\": \"");  json_response.append(prev_val_StatsUpdatedDate); json_response.append("\" }, ");
                        
                        out.println(json_response.toString());
                    }
                    
                    if (format.equals("text"))
                    {
                        text_response.setLength(0);
                        
                        text_response.append(prev_val_Id); text_response.append(" ");
                        text_response.append(prev_val_ColumnName); text_response.append(" ");
                        text_response.append(prev_val_BusinessName); text_response.append(" ");
                        text_response.append(prev_val_SourceColumnName); text_response.append(" ");
                        text_response.append(prev_val_Description); text_response.append(" ");
                        text_response.append(prev_val_DataType); text_response.append(" ");
                        text_response.append(prev_val_DataLength); text_response.append(" ");
                        text_response.append(prev_val_IsNullable); text_response.append(" ");
                        text_response.append(prev_val_IsPrimaryKey); text_response.append(" ");
                        text_response.append(prev_val_IsForeignKey); text_response.append(" ");
                        text_response.append(prev_val_IsMultibyte); text_response.append(" ");
                        text_response.append(prev_val_IsCertified); text_response.append(" ");
                        text_response.append(prev_val_DataClassification); text_response.append(" ");
                        text_response.append(prev_val_UniqueRecords); text_response.append(" ");
                        text_response.append(prev_val_NullRecords); text_response.append(" ");
                        text_response.append(prev_val_DefaultRecords); text_response.append(" ");
                        text_response.append(prev_val_MinValue); text_response.append(" ");
                        text_response.append(prev_val_MaxValue); text_response.append(" ");
                        text_response.append(prev_val_MeanValue); text_response.append(" ");
                        text_response.append(prev_val_StDev); text_response.append(" ");
                        text_response.append(prev_val_MinLength); text_response.append(" ");
                        text_response.append(prev_val_MaxLength); text_response.append(" ");
                        text_response.append(prev_val_AvgLength); text_response.append(" ");
                        text_response.append(prev_val_StatsUpdatedDate); text_response.append(" ");
                        
                        out.println(text_response.toString());
                    }
                    
                } // second pass
                
                
                prev_val_Id = val_Id;
                prev_val_ColumnName = val_ColumnName; 
                prev_val_BusinessName = val_BusinessName; 
                prev_val_SourceColumnName = val_SourceColumnName;            
                prev_val_Description = val_Description; 
                prev_val_DataType = val_DataType; 
                prev_val_DataLength = val_DataLength; 
                prev_val_IsNullable = val_IsNullable; 
                prev_val_IsPrimaryKey = val_IsPrimaryKey; 
                prev_val_IsForeignKey = val_IsForeignKey; 
                prev_val_IsMultibyte = val_IsMultibyte; 
                prev_val_IsCertified = val_IsCertified; 
                prev_val_DataClassification = val_DataClassification; 
                prev_val_UniqueRecords = val_UniqueRecords; 
                prev_val_NullRecords = val_NullRecords; 
                prev_val_DefaultRecords = val_DefaultRecords; 
                prev_val_MinValue = val_MinValue;
                prev_val_MaxValue = val_MaxValue;
                prev_val_MeanValue = val_MeanValue;
                prev_val_StDev = val_StDev;
                prev_val_MinLength = val_MinLength;
                prev_val_MaxLength = val_MaxLength;
                prev_val_AvgLength = val_AvgLength;
                prev_val_StatsUpdatedDate = val_StatsUpdatedDate;           
                
            } // fetching the records

                if (rec_count > 0) // if at least one record, or the last record
                {
                    if (format.equals("json"))
                    { 
                        json_response.setLength(0); // clear the buffer
                        
                        json_response.append("{ \"Id\": \""); json_response.append(prev_val_Id); json_response.append("\"");
                        json_response.append(", \"ColumnName\": \"");  json_response.append(prev_val_ColumnName); json_response.append("\"");
                        json_response.append(", \"BusinessName\": \"");  json_response.append(prev_val_BusinessName); json_response.append("\"");
                        json_response.append(", \"SourceColumnName\": \"");  json_response.append(prev_val_SourceColumnName); json_response.append("\"");
                        json_response.append(", \"Description\": \"");  json_response.append(prev_val_Description); json_response.append("\"");
                        json_response.append(", \"DataType\": \"");  json_response.append(prev_val_DataType); json_response.append("\"");
                        json_response.append(", \"DataLength\": \"");  json_response.append(prev_val_DataLength); json_response.append("\"");
                        json_response.append(", \"IsNullable\": \"");  json_response.append(prev_val_IsNullable); json_response.append("\"");
                        json_response.append(", \"IsPrimaryKey\": \"");  json_response.append(prev_val_IsPrimaryKey); json_response.append("\"");
                        json_response.append(", \"IsForeignKey\": \"");  json_response.append(prev_val_IsForeignKey); json_response.append("\"");
                        json_response.append(", \"IsMultibyte\": \"");  json_response.append(prev_val_IsMultibyte); json_response.append("\"");
                        json_response.append(", \"IsCertified\": \"");  json_response.append(prev_val_IsCertified); json_response.append("\"");
                        json_response.append(", \"DataClassification\": \"");  json_response.append(prev_val_DataClassification); json_response.append("\"");
                        json_response.append(", \"UniqueRecords\": \"");  json_response.append(prev_val_UniqueRecords); json_response.append("\"");
                        json_response.append(", \"NullRecords\": \"");  json_response.append(prev_val_NullRecords); json_response.append("\"");
                        json_response.append(", \"DefaultRecords\": \"");  json_response.append(prev_val_DefaultRecords); json_response.append("\"");
                        json_response.append(", \"MinValue\": \"");  json_response.append(prev_val_MinValue); json_response.append("\"");
                        json_response.append(", \"MaxValue\": \"");  json_response.append(prev_val_MaxValue); json_response.append("\"");
                        json_response.append(", \"MeanValue\": \"");  json_response.append(prev_val_MeanValue); json_response.append("\"");
                        json_response.append(", \"StDev\": \"");  json_response.append(prev_val_StDev); json_response.append("\"");
                        json_response.append(", \"MinLength\": \"");  json_response.append(prev_val_MinLength); json_response.append("\"");
                        json_response.append(", \"MaxLength\": \"");  json_response.append(prev_val_MaxLength); json_response.append("\"");
                        json_response.append(", \"AvgLength\": \"");  json_response.append(prev_val_AvgLength); json_response.append("\"");
                        json_response.append(", \"StatsUpdatedDate\": \"");  json_response.append(prev_val_StatsUpdatedDate); json_response.append("\" } ");
                        
                        out.println(json_response.toString());
                    }
                    
                    if (format.equals("text"))
                    {
                        text_response.setLength(0);
                        
                        text_response.append(prev_val_Id); text_response.append(" ");
                        text_response.append(prev_val_ColumnName); text_response.append(" ");
                        text_response.append(prev_val_BusinessName); text_response.append(" ");
                        text_response.append(prev_val_SourceColumnName); text_response.append(" ");
                        text_response.append(prev_val_Description); text_response.append(" ");
                        text_response.append(prev_val_DataType); text_response.append(" ");
                        text_response.append(prev_val_DataLength); text_response.append(" ");
                        text_response.append(prev_val_IsNullable); text_response.append(" ");
                        text_response.append(prev_val_IsPrimaryKey); text_response.append(" ");
                        text_response.append(prev_val_IsForeignKey); text_response.append(" ");
                        text_response.append(prev_val_IsMultibyte); text_response.append(" ");
                        text_response.append(prev_val_IsCertified); text_response.append(" ");
                        text_response.append(prev_val_DataClassification); text_response.append(" ");
                        text_response.append(prev_val_UniqueRecords); text_response.append(" ");
                        text_response.append(prev_val_NullRecords); text_response.append(" ");
                        text_response.append(prev_val_DefaultRecords); text_response.append(" ");
                        text_response.append(prev_val_MinValue); text_response.append(" ");
                        text_response.append(prev_val_MaxValue); text_response.append(" ");
                        text_response.append(prev_val_MeanValue); text_response.append(" ");
                        text_response.append(prev_val_StDev); text_response.append(" ");
                        text_response.append(prev_val_MinLength); text_response.append(" ");
                        text_response.append(prev_val_MaxLength); text_response.append(" ");
                        text_response.append(prev_val_AvgLength); text_response.append(" ");
                        text_response.append(prev_val_StatsUpdatedDate); text_response.append(" ");
                        
                        out.println(text_response.toString());
                    }
                    
                } // the last record

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
                out.print("GetMaxisColumnsServlet: Cannot close and release connection to the Connection Pool: " + e.getMessage());
                log("GetMaxisColumnsServlet: Cannot close and release connection to the DB Connection Pool: " + e.getMessage());
            } // If connection pooling, return to connection pool; DO NOT set to null                       
            
        } // the BIG try to prepare SQL, extract data , format and return the data
        catch(SQLException ex)
        {
            log("Error: GetMaxisColumnsServlet - SQL exception: " + ex.getMessage());
	    out.println("Error: connecting or executing database command: " + ex.getMessage());            
            response.setStatus(409); 
            response.sendError(409); 
            return;            
        }
        catch (Exception e) 
        {
            log("Error: GetMaxisColumns servlet major error: " + e.getMessage());
	    out.println("Error: GetMaxisColumns servlet major error: " + e.getMessage());            
            response.setStatus(500); 
            response.sendError(500);             
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
