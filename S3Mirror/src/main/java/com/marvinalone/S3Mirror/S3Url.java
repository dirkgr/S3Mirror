package com.marvinalone.S3Mirror;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

class S3Url {
    private static final Pattern s_pattern =
        Pattern.compile("\\As3://([^/]+)/(.*)\\z");
    
    private final String m_bucket;
    private final String m_key;
    
    public S3Url(final String m_bucket, final String m_key) {
        this.m_bucket = m_bucket;
        this.m_key = m_key;
    }
    
    public String getBucket() {
        return m_bucket;
    }
    
    public String getKey() {
        return m_key;
    }
    
    public static S3Url fromString(final String string) {
        final Matcher matcher = s_pattern.matcher(string);
        if(!matcher.find())
            throw new IllegalArgumentException("could not parse S3 location");
        return new S3Url(matcher.group(1), matcher.group(2));
    }

    @Override
    public String toString() {
        return "s3://" + m_bucket + "/" + m_key;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result =
            prime * result + ((m_bucket == null) ? 0 : m_bucket.hashCode());
        result = prime * result + ((m_key == null) ? 0 : m_key.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj)
            return true;
        if(obj == null)
            return false;
        if(getClass() != obj.getClass())
            return false;
        S3Url other = (S3Url)obj;
        if(m_bucket == null) {
            if(other.m_bucket != null)
                return false;
        } else if(!m_bucket.equals(other.m_bucket))
            return false;
        if(m_key == null) {
            if(other.m_key != null)
                return false;
        } else if(!m_key.equals(other.m_key))
            return false;
        return true;
    }
}
