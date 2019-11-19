/*****************************************************************************
 * Class name: ChevelleEnvironment
 * Description: To store the different database, edgenodes and other server names, 
 *              for the different developing, testing and production environments (DEV, TST, CRT and PRD)
 * Author: Dan Grebenisan
 * History: April 2018 - Created
 *****************************************************************************/
package chevelle;


public class ChevelleEnvironment 
{
    
    private String CHEVELLE_ENV = "PRD";
    
    private static final String dev_MaxisDBUrl = "jdbc:sqlserver://epgidvwgds1139.epga.nam.domain.com:1433;DatabaseName=Maxis"; // DEV environment - MAXIS URL of the SQL Server
    private static final String tst_MaxisDBUrl = "jdbc:sqlserver://epgidvwgds1139.epga.nam.domain.com:1433;DatabaseName=Maxis"; // TEST environment - MAXIS URL of the SQL Server
    private static final String crt_MaxisDBUrl = "jdbc:sqlserver://epgidvwedw1031.epga.nam.domain.com:1433;DatabaseName=Maxis"; // CERT (QA) environment - QA MAXIS URL of the SQL Server
    private static final String prd_MaxisDBUrl = "jdbc:sqlserver://dcmipvmedw016.nam.corp.domain.com:1433;DatabaseName=Maxis"; // PROD environment - Milford MAXIS URL of the Warren SQL Server
    // private static final String prd_MaxisDBUrl = "jdbc:sqlserver://dcwipvmedw024.nam.corp.domain.com :1433;DatabaseName=Maxis"; // PROD environment - Warren MAXIS URL of the SQL Server

    
    private static final String dev_EdgeNode_1 = "dcwidvmedg001.edc.nam.domain.com"; // this is DEV_1 Edge Node
    private static final String dev_EdgeNode_2 = "dcwidvmedg002.edc.nam.domain.com"; // this is DEV_2 Edge Node
    
    private static final String tst_EdgeNode_1 = "dcmitvmedg001.edc.nam.domain.com"; // this is TEST_1 Edge Node
    private static final String tst_EdgeNode_2 = "edwtest2edge.int.domain.com"; // this is TEST_2 Edge Node    
    
    private static final String crt_EdgeNode_1 = "dcmipvmedg001.edc.nam.domain.com"; // this is CERT_1 Edge Node
    private static final String crt_EdgeNode_2 = "dcwipvmedg107.edc.nam.domain.com"; // this is CERT_2 Edge Node    
    
    private static final String prd_EdgeNode_1 = "dcmipvmedg001.edc.nam.domain.com"; // this is PROD_1 Edge Node
    private static final String prd_EdgeNode_2 = "dcwipvmedg107.edc.nam.domain.com"; // this is PROD_2 Edge Node       
    
    private static final String sample_data_shell_directory = "/data/commonScripts/util/chevelle_gui/";
    private static final String sample_data_shell = "chevelle_sample_data.sh";  
    
    private static final String validate_regexp_directory = "/data/commonScripts/util/chevelle_gui/";		
    private static final String validate_regexp_data_directory = validate_regexp_directory + "user_data/";
    private static final String validate_regexp_agent = "chevelle_validate_regexp.sh";
    
    private static final int ssh_port = 22;
    
    public void setEnvironment(String env)
    {
        if(env.equals("DEV"))
            CHEVELLE_ENV = "DEV";
        else if(env.equals("TST"))
            CHEVELLE_ENV = "TST";
        else if(env.equals("CRT"))
            CHEVELLE_ENV = "CRT";
        else if(env.equals("PRD"))
            CHEVELLE_ENV = "PRD";        
        else
            CHEVELLE_ENV = "PRD"; // the default
        
    }
    
    public String getEnvironment()
    {
        return CHEVELLE_ENV;
    }
            
    public String getMaxisDBUrl()
    {
        String cur_MaxisUrl = "";
        
        if(CHEVELLE_ENV.equals("DEV"))
            cur_MaxisUrl = dev_MaxisDBUrl;
        else if (CHEVELLE_ENV.equals("TST"))
            cur_MaxisUrl = tst_MaxisDBUrl;
        else if (CHEVELLE_ENV.equals("CRT"))
            cur_MaxisUrl = crt_MaxisDBUrl;        
        else if (CHEVELLE_ENV.equals("PRD"))
            cur_MaxisUrl = prd_MaxisDBUrl;                
        else
            cur_MaxisUrl = prd_MaxisDBUrl;
        
        return cur_MaxisUrl;
    }

    public String getEdgeNode(int sub_env)
    {
        String cur_EdgeNode = "";
        
        if(CHEVELLE_ENV.equals("DEV"))
        {
            if(sub_env == 1)
                cur_EdgeNode = dev_EdgeNode_1;
            else if(sub_env == 2)
                cur_EdgeNode = dev_EdgeNode_2;
            else
                cur_EdgeNode = dev_EdgeNode_1;
        }
        
        else if(CHEVELLE_ENV.equals("TST"))
        {
            if(sub_env == 1)
                cur_EdgeNode = tst_EdgeNode_1;
            else if(sub_env == 2)
                cur_EdgeNode = tst_EdgeNode_2;
            else
                cur_EdgeNode = tst_EdgeNode_1;
        }        
        
        else if(CHEVELLE_ENV.equals("CRT"))
        {
            if(sub_env == 1)
                cur_EdgeNode = crt_EdgeNode_1;
            else if(sub_env == 2)
                cur_EdgeNode = crt_EdgeNode_2;
            else
                cur_EdgeNode = crt_EdgeNode_1;
        }                
        
        else if(CHEVELLE_ENV.equals("PRD"))
        {
            if(sub_env == 1)
                cur_EdgeNode = prd_EdgeNode_1;
            else if(sub_env == 2)
                cur_EdgeNode = prd_EdgeNode_2;
            else
                cur_EdgeNode = prd_EdgeNode_1;
        }                        
        
        else
            cur_EdgeNode = prd_EdgeNode_1;
        
        return cur_EdgeNode;
        
    }
    
    public String getSampleDataShellDirectory()
    {
        return sample_data_shell_directory;
    }
    
    public String getSampleDataShell()
    {
        return sample_data_shell;
    }
    
    public int getSshPort()
    {
        return ssh_port;
    }
    
    public String getValidateRegexpDirectory()
    {
        return validate_regexp_directory;
    }
    
    public String getValidateRegexpDataDirectory() 
    {
        return validate_regexp_data_directory;
    }
    
    public String getValidateRegexpAgent()
    {
        return validate_regexp_agent;
    }
    
}
