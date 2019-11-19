/*****************************************************************************
 * Class name: ChevelleHelper
 * Description: To store the authentication for the different database, edge nodes and other servers, 
 *              for the different developing, testing and production environments (DEV, TST, CRT and PRD)
 * Author: Dan Grebenisan
 * History: April 2018 - Created
 *          May 2 2018 - Added the RSA key for PROD EdgeNodes
 *****************************************************************************/

package chevelle;

public class ChevelleHelper 
{
    // private static String CHEVELLE_ENV = "PRD";
    private String CHEVELLE_ENV = "PRD";
    
    private static final String dev_Maxis_username = "chevelle";
    private static final String dev_Maxis_password = "fG68YVJWveki80oB";   
            
    private static final String tst_Maxis_username = "chevelle";
    private static final String tst_Maxis_password = "fG68YVJWveki80oB";      
    
    private static final String crt_Maxis_username = "chevelle";
    private static final String crt_Maxis_password = "fG68YVJWveki80oC";      
    
    private static final String prd_Maxis_username = "SI_NAM_41100_P";
    private static final String prd_Maxis_password = "19!r4WVth)R^OD3w(YcPYmc1]kG_mQ";                   
    
    private static final String dev_EdgenodeRSAKey = "-----BEGIN RSA PRIVATE KEY-----\n"
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

    private static final String tst_EdgenodeRSAKey = "-----BEGIN RSA PRIVATE KEY-----\n"
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
    
    private static final String crt_EdgenodeRSAKey = "-----BEGIN RSA PRIVATE KEY-----\n"
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
    
    private static final String prd_EdgenodeRSAKey = "-----BEGIN RSA PRIVATE KEY-----\n"
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
    
    
    private static final String dev_auth_code = "MIIEowIBAAKCAQEAnTfUMMtQLQydHydR/eS2RknNwbjwDO3e9Mw/rzN+uZOL45cw"
        + "xmsW7XDq+KpFG3suu1F8O7umitWVADrxahQTQiCYhXXXYBbyHzFTIcEuC+C2sCz3"
        + "s0TT4+J6gzV1HDjdtV86XYrLG5oPTuYLsJHzHxp9l/wvrRxcqMON+D75var9ZKcW"
        + "KubXQyuRvG83Ib0QKg0776Xw5JY5zZ6vAbSkr6RGKNcF2pxk8wZY1c+VHZTPzmgA"
        + "/6qK4rBak1ZNShif+Ip9r1M/Zv2UGcA0zi072FAYYu+lDCdh6W/fZO1a8W2NOeq/"
        + "6BowztYx7cfTF8BR0nABwcMCbmi+a/gCwqvQgwIBJQKCAQAdvnRAlSrYFySlB3Bg"
        + "d2DBMI618o5Hop/Ncr/wtrccMKvDRh38BmwfFVzi78fimsOm5ecSOEIaRBVFO5V0"
        + "7wqP+FQ07MfhvybjToVZabyadp8aaV9LbeLl7JO39VtYXckUeczFlsx61/UO7UB7"
        + "WeHjSjNvx+1YGh9eMtWrgYkxuERodazCeEC5c2HBYmd2JWfO4Puov/IM05JMHF/5"
        + "S1KAcY4ocCn5NuqX50WQU6OcYFRM2jWw6J461Dsa/SJ1cIf7x8EtGZe2BHsllNlb"
        + "BO2TPQkb7d1AfU1ah8Q5UwjOu8wXp95HAfm+3fzQJSPQ3B319T04vDcNpoyTwYKX"
        + "UlMNAoGBAOCmOX7WUchTyXP81Yig27Vvl5V4yOpZGsnSWd417rvVK9oogYbJZFko"
        + "tvPQENegLPZPnbYZseNRhD7OXlW134MKWzWbb+h/963/Uw9ignf0A60hphHX4ftA"
        + "TqJ/vwNai8EYKxJZaMkM0Z2b6dLLFU28d3xr3Rn+rxW38ZsCl+2RAoGBALMokeGZ"
        + "0EsbJM/l/0J+G6O+GZjWVCu1ZkYf02fKpX3qaVgGBKE1FcYVGz54lu75s9LD/8Fj"
        + "dD6jXJo2UEFwnSmWxksA7lmSpRIKo4P4R5L+nBaJFyZNHM3KnMH/Ut/kbt6AI82F"
        + "FK1jbzdzYtVF1mW4w69zJ6DebeY5ULV7qeLTAoGBAKPu66+VefMMtZnULR6DOItR"
        + "bp2Bp17nE4xbOqkSmXRPcwb69rxNxcSFfpY94sbcqzBH76CPSnVy1h/+YIO8Ec5T"
        + "qleNGk+3U9/V/ldVuScZ2StP5+OB3EG5TiONpxBI/jL19erLn330mPZ4sYv7+soG"
        + "EgDZFvdLJdGUESvtIsIdAoGBAKSh0ilBO/H9RGwDx/fpgS6ur7zgoF+EGMrRIyEb"
        + "FJ04RSB0O5sOL68aUGLrWkNh/zBhFIgx4HDpJKliV5YUdL5u37qLO+Obgu378DQU"
        + "lMxD6WDRAIQPgkCQq7kvz51Oc7e0BTlliJ9UdAlw8wkraxFdrOZp0WNknFcEPFO9"
        + "vrTBAoGBAJIWyREW4ogkRGKxhzBB/6kJNoi2aXZ8O6hM43CYzDcr9gF99LTxOTRz"
        + "FFzvzl7UwGSY39U0mNqSZ8qj8YlvyUPWPESDmWVr2FVmq0vxGNUddYzoz0bkYjPo"
        + "mpUjWh8LUNDDzsC6CIUHxhsXHeDDf358yeRA/j0pcUG/L8LfUTGg";
    
    private static final String tst_auth_code = "MIIEowIBAAKCAQEAnTfUMMtQLQydHydR/eS2RknNwbjwDO3e9Mw/rzN+uZOL45cw"
        + "xmsW7XDq+KpFG3suu1F8O7umitWVADrxahQTQiCYhXXXYBbyHzFTIcEuC+C2sCz3"
        + "s0TT4+J6gzV1HDjdtV86XYrLG5oPTuYLsJHzHxp9l/wvrRxcqMON+D75var9ZKcW"
        + "KubXQyuRvG83Ib0QKg0776Xw5JY5zZ6vAbSkr6RGKNcF2pxk8wZY1c+VHZTPzmgA"
        + "/6qK4rBak1ZNShif+Ip9r1M/Zv2UGcA0zi072FAYYu+lDCdh6W/fZO1a8W2NOeq/"
        + "6BowztYx7cfTF8BR0nABwcMCbmi+a/gCwqvQgwIBJQKCAQAdvnRAlSrYFySlB3Bg"
        + "d2DBMI618o5Hop/Ncr/wtrccMKvDRh38BmwfFVzi78fimsOm5ecSOEIaRBVFO5V0"
        + "7wqP+FQ07MfhvybjToVZabyadp8aaV9LbeLl7JO39VtYXckUeczFlsx61/UO7UB7"
        + "WeHjSjNvx+1YGh9eMtWrgYkxuERodazCeEC5c2HBYmd2JWfO4Puov/IM05JMHF/5"
        + "S1KAcY4ocCn5NuqX50WQU6OcYFRM2jWw6J461Dsa/SJ1cIf7x8EtGZe2BHsllNlb"
        + "BO2TPQkb7d1AfU1ah8Q5UwjOu8wXp95HAfm+3fzQJSPQ3B319T04vDcNpoyTwYKX"
        + "UlMNAoGBAOCmOX7WUchTyXP81Yig27Vvl5V4yOpZGsnSWd417rvVK9oogYbJZFko"
        + "tvPQENegLPZPnbYZseNRhD7OXlW134MKWzWbb+h/963/Uw9ignf0A60hphHX4ftA"
        + "TqJ/vwNai8EYKxJZaMkM0Z2b6dLLFU28d3xr3Rn+rxW38ZsCl+2RAoGBALMokeGZ"
        + "0EsbJM/l/0J+G6O+GZjWVCu1ZkYf02fKpX3qaVgGBKE1FcYVGz54lu75s9LD/8Fj"
        + "dD6jXJo2UEFwnSmWxksA7lmSpRIKo4P4R5L+nBaJFyZNHM3KnMH/Ut/kbt6AI82F"
        + "FK1jbzdzYtVF1mW4w69zJ6DebeY5ULV7qeLTAoGBAKPu66+VefMMtZnULR6DOItR"
        + "bp2Bp17nE4xbOqkSmXRPcwb69rxNxcSFfpY94sbcqzBH76CPSnVy1h/+YIO8Ec5T"
        + "qleNGk+3U9/V/ldVuScZ2StP5+OB3EG5TiONpxBI/jL19erLn330mPZ4sYv7+soG"
        + "EgDZFvdLJdGUESvtIsIdAoGBAKSh0ilBO/H9RGwDx/fpgS6ur7zgoF+EGMrRIyEb"
        + "FJ04RSB0O5sOL68aUGLrWkNh/zBhFIgx4HDpJKliV5YUdL5u37qLO+Obgu378DQU"
        + "lMxD6WDRAIQPgkCQq7kvz51Oc7e0BTlliJ9UdAlw8wkraxFdrOZp0WNknFcEPFO9"
        + "vrTBAoGBAJIWyREW4ogkRGKxhzBB/6kJNoi2aXZ8O6hM43CYzDcr9gF99LTxOTRz"
        + "FFzvzl7UwGSY39U0mNqSZ8qj8YlvyUPWPESDmWVr2FVmq0vxGNUddYzoz0bkYjPo"
        + "mpUjWh8LUNDDzsC6CIUHxhsXHeDDf358yeRA/j0pcUG/L8LfUTGg";
    
    private static final String crt_auth_code = "MIIEowIBAAKCAQEAnTfUMMtQLQydHydR/eS2RknNwbjwDO3e9Mw/rzN+uZOL45cw"
        + "xmsW7XDq+KpFG3suu1F8O7umitWVADrxahQTQiCYhXXXYBbyHzFTIcEuC+C2sCz3"
        + "s0TT4+J6gzV1HDjdtV86XYrLG5oPTuYLsJHzHxp9l/wvrRxcqMON+D75var9ZKcW"
        + "KubXQyuRvG83Ib0QKg0776Xw5JY5zZ6vAbSkr6RGKNcF2pxk8wZY1c+VHZTPzmgA"
        + "/6qK4rBak1ZNShif+Ip9r1M/Zv2UGcA0zi072FAYYu+lDCdh6W/fZO1a8W2NOeq/"
        + "6BowztYx7cfTF8BR0nABwcMCbmi+a/gCwqvQgwIBJQKCAQAdvnRAlSrYFySlB3Bg"
        + "d2DBMI618o5Hop/Ncr/wtrccMKvDRh38BmwfFVzi78fimsOm5ecSOEIaRBVFO5V0"
        + "7wqP+FQ07MfhvybjToVZabyadp8aaV9LbeLl7JO39VtYXckUeczFlsx61/UO7UB7"
        + "WeHjSjNvx+1YGh9eMtWrgYkxuERodazCeEC5c2HBYmd2JWfO4Puov/IM05JMHF/5"
        + "S1KAcY4ocCn5NuqX50WQU6OcYFRM2jWw6J461Dsa/SJ1cIf7x8EtGZe2BHsllNlb"
        + "BO2TPQkb7d1AfU1ah8Q5UwjOu8wXp95HAfm+3fzQJSPQ3B319T04vDcNpoyTwYKX"
        + "UlMNAoGBAOCmOX7WUchTyXP81Yig27Vvl5V4yOpZGsnSWd417rvVK9oogYbJZFko"
        + "tvPQENegLPZPnbYZseNRhD7OXlW134MKWzWbb+h/963/Uw9ignf0A60hphHX4ftA"
        + "TqJ/vwNai8EYKxJZaMkM0Z2b6dLLFU28d3xr3Rn+rxW38ZsCl+2RAoGBALMokeGZ"
        + "0EsbJM/l/0J+G6O+GZjWVCu1ZkYf02fKpX3qaVgGBKE1FcYVGz54lu75s9LD/8Fj"
        + "dD6jXJo2UEFwnSmWxksA7lmSpRIKo4P4R5L+nBaJFyZNHM3KnMH/Ut/kbt6AI82F"
        + "FK1jbzdzYtVF1mW4w69zJ6DebeY5ULV7qeLTAoGBAKPu66+VefMMtZnULR6DOItR"
        + "bp2Bp17nE4xbOqkSmXRPcwb69rxNxcSFfpY94sbcqzBH76CPSnVy1h/+YIO8Ec5T"
        + "qleNGk+3U9/V/ldVuScZ2StP5+OB3EG5TiONpxBI/jL19erLn330mPZ4sYv7+soG"
        + "EgDZFvdLJdGUESvtIsIdAoGBAKSh0ilBO/H9RGwDx/fpgS6ur7zgoF+EGMrRIyEb"
        + "FJ04RSB0O5sOL68aUGLrWkNh/zBhFIgx4HDpJKliV5YUdL5u37qLO+Obgu378DQU"
        + "lMxD6WDRAIQPgkCQq7kvz51Oc7e0BTlliJ9UdAlw8wkraxFdrOZp0WNknFcEPFO9"
        + "vrTBAoGBAJIWyREW4ogkRGKxhzBB/6kJNoi2aXZ8O6hM43CYzDcr9gF99LTxOTRz"
        + "FFzvzl7UwGSY39U0mNqSZ8qj8YlvyUPWPESDmWVr2FVmq0vxGNUddYzoz0bkYjPo"
        + "mpUjWh8LUNDDzsC6CIUHxhsXHeDDf358yeRA/j0pcUG/L8LfUTGg";
    
    private static final String prd_auth_code = "MIIEowIBAAKCAQEAnTfUMMtQLQydHydR/eS2RknNwbjwDO3e9Mw/rzN+uZOL45cw"
        + "xmsW7XDq+KpFG3suu1F8O7umitWVADrxahQTQiCYhXXXYBbyHzFTIcEuC+C2sCz3"
        + "s0TT4+J6gzV1HDjdtV86XYrLG5oPTuYLsJHzHxp9l/wvrRxcqMON+D75var9ZKcW"
        + "KubXQyuRvG83Ib0QKg0776Xw5JY5zZ6vAbSkr6RGKNcF2pxk8wZY1c+VHZTPzmgA"
        + "/6qK4rBak1ZNShif+Ip9r1M/Zv2UGcA0zi072FAYYu+lDCdh6W/fZO1a8W2NOeq/"
        + "6BowztYx7cfTF8BR0nABwcMCbmi+a/gCwqvQgwIBJQKCAQAdvnRAlSrYFySlB3Bg"
        + "d2DBMI618o5Hop/Ncr/wtrccMKvDRh38BmwfFVzi78fimsOm5ecSOEIaRBVFO5V0"
        + "7wqP+FQ07MfhvybjToVZabyadp8aaV9LbeLl7JO39VtYXckUeczFlsx61/UO7UB7"
        + "WeHjSjNvx+1YGh9eMtWrgYkxuERodazCeEC5c2HBYmd2JWfO4Puov/IM05JMHF/5"
        + "S1KAcY4ocCn5NuqX50WQU6OcYFRM2jWw6J461Dsa/SJ1cIf7x8EtGZe2BHsllNlb"
        + "BO2TPQkb7d1AfU1ah8Q5UwjOu8wXp95HAfm+3fzQJSPQ3B319T04vDcNpoyTwYKX"
        + "UlMNAoGBAOCmOX7WUchTyXP81Yig27Vvl5V4yOpZGsnSWd417rvVK9oogYbJZFko"
        + "tvPQENegLPZPnbYZseNRhD7OXlW134MKWzWbb+h/963/Uw9ignf0A60hphHX4ftA"
        + "TqJ/vwNai8EYKxJZaMkM0Z2b6dLLFU28d3xr3Rn+rxW38ZsCl+2RAoGBALMokeGZ"
        + "0EsbJM/l/0J+G6O+GZjWVCu1ZkYf02fKpX3qaVgGBKE1FcYVGz54lu75s9LD/8Fj"
        + "dD6jXJo2UEFwnSmWxksA7lmSpRIKo4P4R5L+nBaJFyZNHM3KnMH/Ut/kbt6AI82F"
        + "FK1jbzdzYtVF1mW4w69zJ6DebeY5ULV7qeLTAoGBAKPu66+VefMMtZnULR6DOItR"
        + "bp2Bp17nE4xbOqkSmXRPcwb69rxNxcSFfpY94sbcqzBH76CPSnVy1h/+YIO8Ec5T"
        + "qleNGk+3U9/V/ldVuScZ2StP5+OB3EG5TiONpxBI/jL19erLn330mPZ4sYv7+soG"
        + "EgDZFvdLJdGUESvtIsIdAoGBAKSh0ilBO/H9RGwDx/fpgS6ur7zgoF+EGMrRIyEb"
        + "FJ04RSB0O5sOL68aUGLrWkNh/zBhFIgx4HDpJKliV5YUdL5u37qLO+Obgu378DQU"
        + "lMxD6WDRAIQPgkCQq7kvz51Oc7e0BTlliJ9UdAlw8wkraxFdrOZp0WNknFcEPFO9"
        + "vrTBAoGBAJIWyREW4ogkRGKxhzBB/6kJNoi2aXZ8O6hM43CYzDcr9gF99LTxOTRz"
        + "FFzvzl7UwGSY39U0mNqSZ8qj8YlvyUPWPESDmWVr2FVmq0vxGNUddYzoz0bkYjPo"
        + "mpUjWh8LUNDDzsC6CIUHxhsXHeDDf358yeRA/j0pcUG/L8LfUTGg";
    
    
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
    
    public String getMaxisUsername()
    {
        String cur_MaxisUsername = "";
        
        if(CHEVELLE_ENV.equals("DEV"))
            cur_MaxisUsername = dev_Maxis_username;
        else if (CHEVELLE_ENV.equals("TST"))
            cur_MaxisUsername = tst_Maxis_username;
        else if (CHEVELLE_ENV.equals("CRT"))
            cur_MaxisUsername = crt_Maxis_username;        
        else if (CHEVELLE_ENV.equals("PRD"))
            cur_MaxisUsername = prd_Maxis_username;                
        else
            cur_MaxisUsername = prd_Maxis_username;
        
        return cur_MaxisUsername;        
    }
    
    public String getMaxisPassword()
    {
        String cur_MaxisPassword = "";
        
        if(CHEVELLE_ENV.equals("DEV"))
            cur_MaxisPassword = dev_Maxis_password;
        else if (CHEVELLE_ENV.equals("TST"))
            cur_MaxisPassword = tst_Maxis_password;
        else if (CHEVELLE_ENV.equals("CRT"))
            cur_MaxisPassword = crt_Maxis_password;        
        else if (CHEVELLE_ENV.equals("PRD"))
            cur_MaxisPassword = prd_Maxis_password;                
        else
            cur_MaxisPassword = prd_Maxis_password;
        
        return cur_MaxisPassword;        
    }    
        
    public String getEdgenodeUsername()
    {
        String cur_EdgenodeUsername = "";
        
        if(CHEVELLE_ENV.equals("DEV"))
            cur_EdgenodeUsername = "dedwdlload";
        else if (CHEVELLE_ENV.equals("TST"))
            cur_EdgenodeUsername = "medwdlload";
        else if (CHEVELLE_ENV.equals("CRT"))
            cur_EdgenodeUsername = "medwdlload";        
        else if (CHEVELLE_ENV.equals("PRD"))
            cur_EdgenodeUsername = "pedwdlload";                
        else
            cur_EdgenodeUsername = "pedwdlload";        
        
        return cur_EdgenodeUsername;
    }
    
    public String getEdgenodeRSAKey()
    {
        String cur_EdgenodeRSAKey = "";

        if(CHEVELLE_ENV.equals("DEV"))        
            cur_EdgenodeRSAKey = dev_EdgenodeRSAKey;
        
        else if (CHEVELLE_ENV.equals("TST"))    
            cur_EdgenodeRSAKey = tst_EdgenodeRSAKey;
        
        else if (CHEVELLE_ENV.equals("CRT"))    
            cur_EdgenodeRSAKey = crt_EdgenodeRSAKey;

        else if (CHEVELLE_ENV.equals("PRD"))
            cur_EdgenodeRSAKey = prd_EdgenodeRSAKey;
        
        else
            cur_EdgenodeRSAKey = prd_EdgenodeRSAKey;
        
        return cur_EdgenodeRSAKey;
    }
    
    public String getAuthCode(String cur_sel_env)
    {
        String cur_auth_code = "";

        if(cur_sel_env.equals("DEV"))        
            cur_auth_code = dev_auth_code;
        
        else if (cur_sel_env.equals("TST"))    
            cur_auth_code = tst_auth_code;
        
        else if (cur_sel_env.equals("CRT"))    
            cur_auth_code = crt_auth_code;

        else if (cur_sel_env.equals("PRD"))
            cur_auth_code = prd_auth_code;
        
        else
            cur_auth_code = prd_auth_code;
        
        return cur_auth_code;
    }
    
}
