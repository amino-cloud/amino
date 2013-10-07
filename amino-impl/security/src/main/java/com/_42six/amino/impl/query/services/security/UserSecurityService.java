package com._42six.amino.impl.query.services.security;

import java.util.Arrays;
import java.util.Set;

public class UserSecurityService {
    
    private SecurityService serviceImpl;
    private String aminoAuths;

    public SecurityService getServiceImpl() {
        return serviceImpl;
    }
    public void setServiceImpl(SecurityService serviceImpl) {
        this.serviceImpl = serviceImpl;
    }
    public String getAminoAuths() {
            return this.aminoAuths;
    }
    public void setAminoAuths(String aminoAuths) {
            this.aminoAuths = aminoAuths;
    }

    public String[] getVisibility() {
        Set<String> auth = serviceImpl.getVisibility();
        // Only return the visibilities that amino uses
        auth.retainAll(Arrays.asList(aminoAuths.split(","))); 
        return auth.toArray(new String[auth.size()]);
    }
    public String getUserId() {
        return serviceImpl.getUserId();
    }
    public String getUserName() {
        return serviceImpl.getUserName();
    }
    public boolean isServerCert() {
        return serviceImpl.isServerCert();
    }
}
