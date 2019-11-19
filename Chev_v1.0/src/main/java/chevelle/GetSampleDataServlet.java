/**************************************************************************
 * Servlet: GetSampleDataServlet
 * URL: /GetSampleData
 * Call method (prefered): GET
 * Parameters:
 *  - server: P1 or P2
 *  - hdfs_path: hadoop location of the table
 *  - no_of_records: number of records
 *  - format: the output format of the response (json, text)
 * Author: Dan Grebenisan
 * History: - Feb 2018 - Created
 *          - Mar 5, 2018 - Changed the JSch library with Ganymend
 *          - Mar 26, 2018 - Updated the HTTP error codes
 *          - Apr 16, 2018 - added code for the currently running environment
 ***************************************************************************/
package chevelle;

import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import java.text.DateFormat.*;
import java.io.ByteArrayInputStream;
import java.text.SimpleDateFormat;

import java.util.Date;
import java.util.Random;
import org.apache.commons.io.*;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.SCPClient;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;

import java.nio.charset.StandardCharsets;
import javax.naming.Context;
import javax.naming.InitialContext;

import chevelle.ChevelleEnvironment;
import chevelle.ChevelleHelper;

public class GetSampleDataServlet extends HttpServlet 
{
  
    // String edge_node_p1 = "dcwidvmedg001.edc.nam.domain.com"; // this is DEV_1. the real PROD_1 is dcmipvmedg001.edc.nam.domain.com
    // String edge_node_p2 = "dcwidvmedg002.edc.nam.domain.com"; // this is DEV_2, the real PROD_2 is dcwipvmedg107.edc.nam.domain.com
    
    String edge_node_p1 = null;
    String edge_node_p2 = null;

/*    
    private String keyStr = "-----BEGIN RSA PRIVATE KEY-----\n"
    + "MIIEowIBAAKCAQEAnTfUMMtQLQydHydR/eS2RknNwbjwDO3e9Mw/rzN+uZOL45cw\n"
    + "xmsW7XDq+KpFG3suu1F8O7umitWVADrxahQTQiCYhXXXYBbyHzFTIcEuC+C2sCz3\n"
    + "s0TT4+J6gzV1HDjdtV86XYrLG5oPTuYLsJHzHxp9l/wvrRxcqMON+D75var9ZKcW\n"
    + "KubXQyuRvG83Ib0QKg0776Xw5JY5zZ6vAbSkr6RGKNcF2pxk8wZY1c+VHZTPzmgA\n"
    + "/6qK4rBak1ZNShif+Ip9r1M/Zv2UGcA0zi072FAYYu+lDCdh6W/fZO1a8W2NOeq/\n"
    + "6BowztYx7cfTF8BR0nABwcMCbmi+a/gCwqvQgwIBJQKCAQAdvnRAlSrYFySlB3Bg\n"
    + "d2DBMI618o5Hop/Ncr/wtrccMKvDRh38BmwfFVzi78fimsOm5ecSOEIaRBVFO5V0\n"
    + "7wqP+FQ07MfhvybjToVZabyadp8aaV9LbeLl7JO39VtYXckUeczFlsx61/UO7UB7\n"
    + "WeHjSjNvx+1YGh9eMtWrgYkxuERodazCeEC5c2HBYmd2JWfO4Puov/IM05JMHF/5\n"
    + "S1KAcY4ocCn5NuqX50WQU6OcYFRM2jWw6J461Dsa/SJ1cIf7x8EtGZe2BHsllNlb\n"
    + "BO2TPQkb7d1AfU1ah8Q5UwjOu8wXp95HAfm+3fzQJSPQ3B319T04vDcNpoyTwYKX\n"
    + "UlMNAoGBAOCmOX7WUchTyXP81Yig27Vvl5V4yOpZGsnSWd417rvVK9oogYbJZFko\n"
    + "tvPQENegLPZPnbYZseNRhD7OXlW134MKWzWbb+h/963/Uw9ignf0A60hphHX4ftA\n"
    + "TqJ/vwNai8EYKxJZaMkM0Z2b6dLLFU28d3xr3Rn+rxW38ZsCl+2RAoGBALMokeGZ\n"
    + "0EsbJM/l/0J+G6O+GZjWVCu1ZkYf02fKpX3qaVgGBKE1FcYVGz54lu75s9LD/8Fj\n"
    + "dD6jXJo2UEFwnSmWxksA7lmSpRIKo4P4R5L+nBaJFyZNHM3KnMH/Ut/kbt6AI82F\n"
    + "FK1jbzdzYtVF1mW4w69zJ6DebeY5ULV7qeLTAoGBAKPu66+VefMMtZnULR6DOItR\n"
    + "bp2Bp17nE4xbOqkSmXRPcwb69rxNxcSFfpY94sbcqzBH76CPSnVy1h/+YIO8Ec5T\n"
    + "qleNGk+3U9/V/ldVuScZ2StP5+OB3EG5TiONpxBI/jL19erLn330mPZ4sYv7+soG\n"
    + "EgDZFvdLJdGUESvtIsIdAoGBAKSh0ilBO/H9RGwDx/fpgS6ur7zgoF+EGMrRIyEb\n"
    + "FJ04RSB0O5sOL68aUGLrWkNh/zBhFIgx4HDpJKliV5YUdL5u37qLO+Obgu378DQU\n"
    + "lMxD6WDRAIQPgkCQq7kvz51Oc7e0BTlliJ9UdAlw8wkraxFdrOZp0WNknFcEPFO9\n"
    + "vrTBAoGBAJIWyREW4ogkRGKxhzBB/6kJNoi2aXZ8O6hM43CYzDcr9gF99LTxOTRz\n"
    + "FFzvzl7UwGSY39U0mNqSZ8qj8YlvyUPWPESDmWVr2FVmq0vxGNUddYzoz0bkYjPo\n"
    + "mpUjWh8LUNDDzsC6CIUHxhsXHeDDf358yeRA/j0pcUG/L8LfUTGg\n"
    + "-----END RSA PRIVATE KEY-----";
*/
    // private static char[] key = keyStr.toCharArray();    
    
    private String user = null;
    private char[] key = null; 
    
    javax.naming.InitialContext initContext = null;
    javax.naming.Context envContext = null;
    ServletContext srvContext = null;
    
    String CHEVELLE_ENV = null;
    String sys_env_var = null;
    String init_context_env = null;

    ChevelleEnvironment chevelleEnvironment = null;
    ChevelleHelper chevelleHelper = null;    

    @Override
    public void init() throws ServletException 
    {
        srvContext = getServletContext();
        
        chevelleEnvironment = new ChevelleEnvironment();
        chevelleHelper = new ChevelleHelper(); 
        
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
     
        chevelleEnvironment.setEnvironment(CHEVELLE_ENV);
        chevelleHelper.setEnvironment(CHEVELLE_ENV);      
        
        user = chevelleHelper.getEdgenodeUsername();
        key = chevelleHelper.getEdgenodeRSAKey().toCharArray();
        
        edge_node_p1 = chevelleEnvironment.getEdgeNode(1);
        edge_node_p2 = chevelleEnvironment.getEdgeNode(2);
        
    }


    @Override
    public void destroy()
    {
            try {envContext.close();}catch(Exception ignore){}finally{envContext = null;}
            try {initContext.close();}catch(Exception ignore){}finally{initContext = null;}        
    }
    
    
    
    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException 
    {
        
        ch.ethz.ssh2.Connection conn = null;
        ch.ethz.ssh2.SCPClient scp = null;
        ch.ethz.ssh2.Session sess = null;
        
        PrintWriter out = null;      
        
        // read again the currently running environment from the servlet context
        // if different from the existing one, call init() to reload the new environment
        
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
            log("Chevelle currently running environment reloaded: " + CHEVELLE_ENV);
        }
        
        String server = request.getParameter("server");
        if(server == null)
            server = ""; // environment: D1, D2 for development, P1 or P2 for production 
        else
        {
            if (server.equals("P1") || server.equals("p1"))
                // server = edge_node_p1;
                server = chevelleEnvironment.getEdgeNode(1);
            if (server.equals("P2") || server.equals("p2"))
                // server = edge_node_p2;    
                server = chevelleEnvironment.getEdgeNode(2);
        }
        
        String hdfs_path = request.getParameter("hdfs_path");
        if(hdfs_path == null)
            hdfs_path = ""; // hadoop path of the table      
        
        String no_of_records = request.getParameter("no_of_records");
        if(no_of_records == null)
            no_of_records = ""; // number of records to retrieve, 20, 50, 100      
        
        String format = request.getParameter("format");
        if(format == null)
            format = "json"; // html, xml, json, text
        
        int port = chevelleEnvironment.getSshPort();
        
        String remote_directory = chevelleEnvironment.getSampleDataShellDirectory();
        String remoteShell = chevelleEnvironment.getSampleDataShell();
        
        try
        {
            out = response.getWriter();
            // if most of these parameters are empty, return the servlet
            if (hdfs_path.equals(""))
            {
                out.close();
                return;
            }
            // ... continue checking for empty
        }
        catch(Exception ex)
        {
            log("Error: GetSampleDataServlet - Cannot open a stream for the client: " + ex.getMessage());
            response.setStatus(500);
            response.sendError(500);
            out = null;
            return;
        }
        
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
            format = "text";
            response.setContentType("text/plain;charset=UTF-8");
        }
        
        
        // String user = "dedwdlload";
        try // open and execute the SSH connection
        {
            conn = new Connection(server);
            conn.connect();
            // boolean isAuthenticated = conn.authenticateWithPassword(user, passcode); // just for testing
            boolean isAuthenticated = conn.authenticateWithPublicKey(user, key, null);
            
            if (isAuthenticated == false)
		throw new IOException("RSA authentication failed.");
        }
        catch (Exception ex)
        {
            log("Error: GetSampleDataServlet - EdgeNode RSA authentication error: " + ex.getMessage());
            out.println("{ \"r\": \"" + "Error: EdgeNode RSA authentication: " + ex.getMessage() + "\" } ");
            response.setStatus(401); // Un-authorized
            response.sendError(401); // Un-authorized
            conn = null;
            return;
        } 
         
        // by now the session is open
        // create an execution channel and run the command
        try
        {
            
            String line;
            String err_line;          
            
            StringBuilder sb_line = new StringBuilder();
            StringBuilder sb_text_line = new StringBuilder();
            StringBuilder sb_pre_line = new StringBuilder();
            sb_line.setLength(0);
            sb_pre_line.setLength(0);
            
            int dec_field_delim = 28;  // DELIMITED FIELDS TERMINATED BY '\034'
            int line_count = 0;
            
            sess = conn.openSession();
            // String command = "./chevelle_sample_data.sh /IC/VEHICLE_COST/VEHICLE_SUPPLEMENTAL_COST/Data 50";
            String command = remote_directory + remoteShell + " " + hdfs_path + " " + no_of_records;      
            sess.execCommand(command);
            
            InputStream stdout = new StreamGobbler(sess.getStdout());
            InputStream stderr = new StreamGobbler(sess.getStderr());

            BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(stdout));
            BufferedReader stderrReader = new BufferedReader(new InputStreamReader(stderr));

            String delim = String.valueOf(Character.toChars(dec_field_delim));  // this works too   
            
            while ((line = stdoutReader.readLine()) != null)
            { 
                line_count++;
                
                if (line_count > 1 && format.equals("json"))
                {
                    sb_pre_line.append(" ], ");                
                    out.println(sb_pre_line.toString());
                    out.flush();                    
                }                                
                
                // parse the line in tokens and retunr in JSON format
                String[] tokens = line.split(delim);
                sb_line.setLength(0);
                sb_pre_line.setLength(0);
                
                sb_text_line.setLength(0);
                
                for (int i = 0; i < tokens.length; i++)
                {
                    if(format.equals("json"))
                    {
                        if (i == 0) // first token
                        {
                            sb_line.append("{ \"c\": \""); 
                            sb_line.append(tokens[i]);
                            sb_line.append("\" }");
                        }
                        else
                        {
                            sb_line.append(", { \"c\": \"");
                            sb_line.append(tokens[i]);
                            sb_line.append("\" }");
                        }
                    }
                    if (format.equals("text"))
                    {
                        sb_text_line.append(tokens[i]);
                        sb_text_line.append(";");
                    }
                }

                // pre_full_line = "[ " + sb_line.toString() + " ], ";
                sb_pre_line.append("[ ");                    
                sb_pre_line.append(sb_line);
                // sb_pre_line.append(" ], ");                

                if (format.equals("text"))        
                {
                    out.println(sb_text_line.toString());
                    out.flush();
                }
            }
      
            if (line_count > 0 && format.equals("json"))
            {
                sb_pre_line.append(" ]");
                out.println(sb_pre_line.toString());
            }      
            
            while ((err_line = stderrReader.readLine()) != null)
            {
                if (!err_line.contains("Unable to write to output stream"))
                    out.println("{ \"c\": \"Error: " + err_line + "\" }");
            }
            
            
                        
            if (format.equals("json"))
                out.println(" ]");
            
            out.flush();
            
            stdoutReader.close();
            stderrReader.close();
            
            sess.close();
            conn.close();
            
        }
        catch (Exception ex)
        {
            log("SSH execution error: " + ex.getMessage());
            // out.println("SSH execution error: " + ex.getMessage());
            out.println("{ \"r\": \"" + "Error: SSH execution: " + ex.getMessage() + "\" } ");
        }
        finally 
	{
            if (sess != null)
            {
            	sess.close();
                sess = null;
            }
            if (conn != null)
            {
            	conn.close();
                conn = null;
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
