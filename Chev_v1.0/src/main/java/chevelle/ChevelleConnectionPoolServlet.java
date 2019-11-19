/*****************************************************************************************
 * Servlet name: ChevelleConnectionPoolServlet
 * Href: /ChevelleConnectionPool
 * Description: It creates a database connection pooling for Maxis DB 
 *              based on the currently running environment provided by the ChevelleEnv
 *              The admin can call this servlet using the /ChevelleConnectionPool.html driver page, 
 *              to reload the currently running environment, in case of dynamic change
 *              This servlet must be started always AFTER the ChevelleEnvServlet
 * Author: Dan Grebenisan
 * History: April 2018 - Created
 *****************************************************************************************/
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
import javax.servlet.ServletException;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.tomcat.jdbc.pool.DataSource; //test to see if it works in the cloud
import org.apache.tomcat.jdbc.pool.PoolProperties;

import chevelle.ChevelleEnvironment;
import chevelle.ChevelleHelper;


/**
 *
 * @author BZY91Q
 */
public class ChevelleConnectionPoolServlet extends HttpServlet 
{

    String CHEVELLE_ENV = null;
        
    InitialContext initContext = null;
    javax.naming.Context envContext = null;
    ServletContext srvContext = null;
    
    org.apache.tomcat.jdbc.pool.DataSource ds = null;
    boolean status = false; // the status of the Conection Pooling; false: CP not ready; true: CP ready to go
    
    ChevelleEnvironment chevelleEnvironment = null;
    ChevelleHelper chevelleHelper = null;
    
    String sys_env_var = null;
    String init_context_env = null;
    
    @Override
    public void init() throws ServletException 
    {
        
        java.sql.Connection con = null;
        java.sql.Statement stmt = null;
        java.sql.ResultSet rs = null;
        
        chevelleEnvironment = new ChevelleEnvironment();
        chevelleHelper = new ChevelleHelper();     
        
        srvContext = getServletContext();
        
        try
        {
            
            CHEVELLE_ENV = (java.lang.String) srvContext.getAttribute("CHEVELLE_ENV");
            status = true;
            log("Chevelle environment successfully retrieved from the servlet context: " + CHEVELLE_ENV);
        }
        catch (Exception e)
        {
            log("Cannot retrieve the CHEVELLE_ENV: " + e.getMessage());
            CHEVELLE_ENV = null;
            status = false;
        }

        if (CHEVELLE_ENV == null || CHEVELLE_ENV.equals("") || CHEVELLE_ENV.equals("null"))
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
                status = false;
            }               

            if (sys_env_var != null && !sys_env_var.equals(""))
            {
                CHEVELLE_ENV = sys_env_var; // a valid system environment variable 
                status = true;
                log("Chevelle environment overwrite with system variable: " + CHEVELLE_ENV);
            }
            else // didn't get anything from the system environment, try the initial context
            {
                log("Chevelle local system variable is null");
                // now try to read the initial context
                try
                {
                    initContext = new InitialContext();
                    envContext = (Context)initContext.lookup("java:/comp/env");
                    init_context_env = (String) envContext.lookup("CHEVELLE_ENV");
                    
                    if(init_context_env != null && !init_context_env.equals("") )
                    {
                        CHEVELLE_ENV = init_context_env;
                        status = true;
                        log("Chevelle initial context look-up success: " + init_context_env);
                    }
                    else
                    {
                        CHEVELLE_ENV = null;
                        status = false;
                        log("Chevelle initial context look-up null or empty ");
                    }
                }
                catch (Exception e)
                {
                    log("Chevelle initial context look-up error: " + e.getMessage());
                    CHEVELLE_ENV = null;
                    init_context_env = null;
                    status = false;
                }                
            }        

            // at this point if CHEVELLE_ENV is still null, just initialize it with the default for PRODUCTION
            if(CHEVELLE_ENV == null || CHEVELLE_ENV.equals("") || CHEVELLE_ENV.equals("null"))
            {
                CHEVELLE_ENV = "PRD";  
                status = true;
            }

        }
        
        // now we're good, we have a value from the ServletContext
        
        chevelleEnvironment.setEnvironment(CHEVELLE_ENV);
        chevelleHelper.setEnvironment(CHEVELLE_ENV);
            
        // now initialize the Conenctin Pooling

        PoolProperties p = new PoolProperties();

        p.setTestWhileIdle(false);
        p.setTestOnBorrow(true);
        p.setTestOnReturn(false);
        p.setValidationQuery("SELECT 1");
        p.setValidationInterval(30000);
        p.setTimeBetweenEvictionRunsMillis(30000);
        p.setMaxActive(10);
        p.setMinIdle(3);
        p.setMaxWait(30000);
        p.setInitialSize(5);
        p.setRemoveAbandonedTimeout(60);
        p.setRemoveAbandoned(true);
        p.setLogAbandoned(false);
        p.setMinEvictableIdleTimeMillis(60000);
        p.setJmxEnabled(true);
        p.setJdbcInterceptors("org.apache.tomcat.jdbc.pool.interceptor.ConnectionState;"+
                "org.apache.tomcat.jdbc.pool.interceptor.StatementFinalizer");    
        p.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver");        
        // p.setUrl("jdbc:sqlserver://epgidvwgds1139.epga.nam.domain.com:1433;DatabaseName=Maxis");      
        p.setUrl(chevelleEnvironment.getMaxisDBUrl());
        // p.setUsername("chevelle");
        p.setUsername(chevelleHelper.getMaxisUsername());
        // p.setPassword("fG68YVJWveki80oB");   
        p.setPassword(chevelleHelper.getMaxisPassword());
        p.setMaxIdle(5);
        p.setMaxWait(120000);

        // p.setUsername("chevelle"); // for the development environment
        // p.setPassword("fG68YVJWveki80oB"); // for the development environment

        try
        {
            ds = new DataSource();
            ds.setPoolProperties(p);

            con = ds.getConnection();
            status = true;
            log("Chevelle pooled DataSource loaded and connected successfully!"); 
        }
        catch(Exception e)
        {
            log("Error: cannot get a connection from the Chevelle pool: " + e.getMessage());
            status = false;
            return;
        }

        try
        {
            stmt = con.createStatement();
            stmt.setQueryTimeout(10);
            String sql = "SELECT 1";                
            rs = stmt.executeQuery(sql);   
            log("Chevelle pool: test SELECT ran successfully!");
        }
        catch(Exception e)
        {
            log("Error: test SELECT on Chevelle pool: " + e.getMessage());
            status = false;
            return;
        }

        try 
        {
            rs.close();
            rs = null;
            stmt.close();
            stmt = null;                

            con.close();
            log("Connection released successfully to the Connection Pool");
        } 
        catch(Exception e)
        {
            log("Error: Cannot close and release connection to the Connection Pool: " + e.getMessage());
        }

        try
        {
            ServletContext context = getServletContext();
            status = true;
            context.setAttribute("chevelle_connection_pool", ds);  
            log("Connection Pooling successfully attached to the Servlet Context!");

        }
        catch(Exception e)
        {
            status = false;
            log("Error: Connection Pooling could not be attached to the Servlet Context: " + e.getMessage());
        }


    }
    
    @Override
    public void destroy()
    {
            ds.close();
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
        // send back its own interface
        // only if the method is post and the load parameter exists and its true, and the auth_code is correct, 
        // then reload first the CHEVELLE_ENV from the servlet context, and then reload the connection pooling for the new environment
        String action_type = request.getParameter("action_type");
        String auth_code = request.getParameter("auth_code");
        String action_message = "";
        
        if ( action_type != null && auth_code != null)
        {
            if( request.getMethod().equals("POST") && action_type.equals("reload") )
            {
                if ( auth_code.equals(chevelleHelper.getAuthCode(CHEVELLE_ENV)) )
                {
                    init();
                    if (status == true)
                    {
                        log("The Chevelle connection pooling has been reloaded successfully! The currenly running environment is: " + CHEVELLE_ENV);
                        action_message = "The Chevelle connection pooling has been reloaded successfully!<br /> The currenly running environment is: " + CHEVELLE_ENV; 
                    }
                    else
                    {
                        log("Error: the Chevelle connection pooling could not be reloaded! The currenly running environment is: " + CHEVELLE_ENV);
                        action_message = "Error: the Chevelle connection pooling could not be reloaded!<br /> The currenly running environment is: " + CHEVELLE_ENV; 
                    }
                }
                else
                   action_message = "You are not authorised to reload the connection pooling !!!!<br /> The currenly running environment is: " + CHEVELLE_ENV; 
            }
        }
        
        response.setContentType("text/html;charset=UTF-8");
        PrintWriter out = response.getWriter();
        
            out.println("<!DOCTYPE html>");
            out.println("<html>");
            out.println("<head>");
            out.println("<title>Servlet ChevelleConnectionPoolServlet</title>");  
            
            out.println("<script>");
            out.println("function OnRefreshStatus() { ");
            out.println("document.getElementById(\"action_type\").value = \"refresh\";");
            out.println("document.getElementById(\"form_connection_pool\").submit();");
            out.println(" } ");
            out.println("function OnReloadEnvironment() { ");
            out.println("document.getElementById(\"action_type\").value = \"reload\";");
            out.println("document.getElementById(\"form_connection_pool\").submit();");
            out.println("} ");
            out.println("</script>");
           
            out.println("</head>");
            out.println("<body>");
            out.println("<h3>Servlet ChevelleConnectionPoolServlet at " + request.getContextPath() + "</h3>");
            if(status == true)
                out.println("<h4>Chevelle Connection Pool is up and running!</h4>");
            else
                out.println("<h4>Error: Chevelle Connection Pool could not be initialised!</h4>");
            
            out.println("<p>The currently running environment of the Chevelle Connection Pooling is: <span style=\"font-weight: bold; font-size: 14pt;\"  >" + CHEVELLE_ENV + "</span></p>");
            out.println("<form id=\"form_connection_pool\" name=\"form_connection_pool\" method=\"post\" >");
            out.println("<div>");
            out.println("To reload the most updated environment, enter the authorization code, and click \"Reload Chevelle ENV\":<br />");
            out.println("<textarea id='auth_code' name='auth_code' style=\"width:700px; height:350px; font-family:'Courier New'; font-size:11pt; \"></textarea>");
            out.println("</div>");
            out.println("<div style=\"text-align: center; width: 700px; margin-top: 10px;\">");
            out.println("<input id=\"action_type\" name=\"action_type\" type=\"hidden\" value=\"refresh\" />");
            out.println("<input id=\"get_refresh\" type=\"button\" value=\"Refresh status\" onclick=\"OnRefreshStatus()\" />");
            out.println("<input id=\"reload\" type=\"button\" value=\"Reload Chevelle ENV\" onClick=\"OnReloadEnvironment()\" />");
            out.println("</div>");
            out.println("</form>");
            out.println(action_message);

            
            out.println("</body>");
            out.println("</html>");
        
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
