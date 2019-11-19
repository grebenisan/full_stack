/*****************************************************************************************
 * Servlet name: ChevelleEnvServlet
 * Href: /ChevelleEnv
 * Description: It decides what is the currently running environment 
 *              and it supplies this value to all the other servlets
 *              The admin can call this servlet using the /ChevelleEnv.html driver page, 
 *              to dynamically change the currently running environment
 * Author: Dan Grebenisan
 * History: April 2018 - Created
 *****************************************************************************************/
package chevelle;

import java.io.IOException;
import java.io.PrintWriter;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

// import chevelle.ChevelleEnvironment;
import chevelle.ChevelleHelper;


/**
 *
 * @author BZY91Q
 */
public class ChevelleEnvServlet extends HttpServlet {

   
    javax.naming.InitialContext initContext = null;
    javax.naming.Context envContext = null;
    private String CHEVELLE_ENV = null;
    private String init_context_env = null;
    private String sys_env_var = null;
    private ServletContext servContext = null;
    private ChevelleHelper chevelleHelper = null;        
    
    boolean status = true;
    // ChevelleEnvironment chevelleEnvironment = null;
    
    @Override
    public void init() throws ServletException 
    {

        servContext = getServletContext();
        chevelleHelper = new ChevelleHelper();
        
        // try first to retrieve from the application context
        try
        {
            initContext = new InitialContext();
            envContext = (Context)initContext.lookup("java:/comp/env");
            
            init_context_env = (String) envContext.lookup("CHEVELLE_ENV");
            if(init_context_env != null)
                CHEVELLE_ENV = init_context_env;
            //ChevelleEnvironment.setEnvironment(CHEVELLE_ENV);
            //ChevelleHelper.setEnvironment(CHEVELLE_ENV);

            log("Chevelle initial context look-up success: " + init_context_env);
 
        }
        catch (Exception e)
        {
            log("Chevelle initial context look-up error: " + e.getMessage());
            CHEVELLE_ENV = null;
            init_context_env = null;
            status = false;
        }

        // try to retrieve the environment variable. if successfull, overwrite the CHEVELLE_ENV
        // String sysenv_CHEVELLE_ENV = null;
        try
        {
            sys_env_var = System.getenv("CHEVELLE_ENV");
               log("Chevelle environment variable look-up success: " + sys_env_var);
        }
        catch (Exception e)
        {
            log("Chevelle environment variable look-up error: " + e.getMessage());
            sys_env_var = null;
            status = false;

        }   

        if (sys_env_var != null && !sys_env_var.equals(""))
        {
            CHEVELLE_ENV = sys_env_var; // a local environment variable would overwrite the context variable
            log("Chevelle environment overwrite with system variable!");
        }
        else
        {
            log("Chevelle local system variable is null");
        }
        
        if (CHEVELLE_ENV == null) 
        {
            CHEVELLE_ENV = "PRD"; // default to PRD in case the CHEVELLE_ENV cannot be initialized from the other sources
            log("CHEVELLE_ENV was null, initialized with default \"PROD\"");
        }
        
        servContext.setAttribute("CHEVELLE_ENV", CHEVELLE_ENV); // make it available to the other servlets
        chevelleHelper.setEnvironment(CHEVELLE_ENV);
    }
    
    @Override
    public void destroy()
    {
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
        String action_message = "";
        
        response.setContentType("text/html;charset=UTF-8");
        PrintWriter out = response.getWriter();
        
        // get the request parameters, if any
            
        String cur_env = request.getParameter("cur_env");
        if (cur_env == null)
        {
            cur_env = CHEVELLE_ENV;
        }

        
        String auth_code = request.getParameter("auth_code");
        if (auth_code == null)
            auth_code = "";
        
        // log(auth_code);
        
        String action_type = request.getParameter("action_type");
        if (action_type == null)
            action_type = "";
        
        if(request.getMethod().equals("GET"))
        {
            cur_env = CHEVELLE_ENV;
            action_message = ""; // do nothing, just refresh the page with the current values
        }
        
        if(request.getMethod().equals("POST") && action_type.equals("refresh"))
        {
            cur_env = CHEVELLE_ENV;
            action_message = "Refresh! No action performed."; // do nothing, just refresh the page with the current values 
        }
        
        if(request.getMethod().equals("POST") && action_type.equals("change"))
        {
            
            if (cur_env.equals(CHEVELLE_ENV))
            {
                // log("Same env value selected as the currently running environment. No action required");
                action_message = "Same value selected for the currently running environment. No action required";
            }
            else
            {
                // log("Action New env value selected to change the currently running environment: " + cur_env);
                
                
                // act only of the auth_code mathes the ChevelleHelper key for the current env
                if (auth_code.equals(chevelleHelper.getAuthCode(cur_env)))
                {
                    CHEVELLE_ENV = cur_env;
                    
                    // set the new value in the Servlet context
                    servContext.setAttribute("CHEVELLE_ENV", CHEVELLE_ENV); // set with the new value
                    
                    chevelleHelper.setEnvironment(CHEVELLE_ENV); // update the helper class   
                    
                    // prepare the action_message, depending on the action      
                    action_message = "The currently running environment has been successfully changed to: " + cur_env;
                    log(action_message);
                    
                }
                else
                {
                    action_message = "You are not authorised to change the currently running environment: " + CHEVELLE_ENV + "!<br /> Action denied!";
                    cur_env = CHEVELLE_ENV;
                }
                
            }
                

        }
    
        
        
        /* TODO output your page here. You may use following sample code. */
        out.println("<!DOCTYPE html>");
        out.println("<html>");
        out.println("<head>");
        out.println("<title>Servlet ChevelleEnvServlet</title>");   
        
        out.println("<script>");
        out.println("function OnRefreshStatus() { ");
        out.println("document.getElementById(\"action_type\").value = \"refresh\"; ");
        out.println("document.getElementById(\"form_chevelle_env\").submit(); }");
        out.println("");
        out.println("function OnChangeEnvironment() { ");
        out.println("document.getElementById(\"action_type\").value = \"change\"; ");
        out.println("document.getElementById(\"form_chevelle_env\").submit(); }");
        out.println("</script>");
        
        out.println("</head>");
        out.println("<body>");

        out.println("<h3>Servlet ChevelleEnvServlet at " + request.getContextPath() + "</h3>");

        out.println("<table><tr>");
        out.println("<td>The currently running Chevelle Environment is: </td>");
        out.println("<td style=\"font-weight: bold; font-size: 14pt;\">" + CHEVELLE_ENV + "</td>");
        out.println("</tr><tr>");
        out.println("<td>The CHEVELLE_ENV application context is: </td>");
        out.println("<td>" + init_context_env + "</td>");
        out.println("</tr><tr>");
        out.println("<td>The CHEVELLE_ENV system variable is:</td>");
        out.println("<td>" + sys_env_var + "</td>");
        out.println("</tr></table><br />");
        
        out.println("<form id=\"form_chevelle_env\" name=\"form_chevelle_env\" method=\"post\" >");
        
        out.println("<div>SET the currently running environment:");
        out.println("<select name=\"cur_env\" id=\"cur_env\" style=\"width:100px; font-family:'Courier New'; font-size:12pt;\" >");
        
        if (cur_env.equals("DEV"))
            out.println("<option selected>DEV</option>");
        else
            out.println("<option>DEV</option>");
        
        if (cur_env.equals("TST"))
            out.println("<option selected>TST</option>");
        else
            out.println("<option>TST</option>");
        
        if (cur_env.equals("CRT"))
            out.println("<option selected>CRT</option>");
        else
            out.println("<option>CRT</option>");
        
        if (cur_env.equals("PRD"))
            out.println("<option selected>PRD</option>");
        else
            out.println("<option>PRD</option>");
        
        out.println("</select></div>");
        
        out.println("<div>Enter the authorization code:<br />");
        out.println("<textarea id='auth_code' name='auth_code' style=\"width:700px; height:350px; font-family:'Courier New'; font-size:11pt;  \"></textarea>");
        out.println("</div>");
        
        out.println("<div style=\"text-align: center; width: 700px; margin-top: 10px;\">");
        out.println("<input id=\"action_type\" name=\"action_type\" type=\"hidden\" value=\"refresh\" />");
        out.println("<input id=\"get_refresh\" type=\"button\" value=\"Refresh status\" onclick=\"OnRefreshStatus()\" />");
        out.println("<input id=\"post_set\" type=\"button\" value=\"Set current Chevelle Environment\" onClick=\"OnChangeEnvironment()\" />");
        out.println("</div>");
        out.println("</form>");        
        
        // out.println("<p>" + auth_code + "</p>");        
        
        out.println("<p>" + action_message + "</p>");
        
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
