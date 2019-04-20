export interface JsonWebToken {
    aud: string[];
    auth_time: number;
    azp: string;
    cid: string;
    client_id: string;
    email: string;
    exp: number;
    iat: number;
    iss: string;
    jti: string;
    origin: string;
    rev_sig: string;
    scope: string[];
    sub: string;
    user_id: string;
    user_name: string;
    zid: string;
  }
  