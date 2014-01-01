package com._42six.amino.query.exception;

/**
 * Top level exception thrown by BigTable classes
 */
public class BigTableException extends Exception {
    public BigTableException(){
        super();
    }

    public BigTableException(String s) {
        super(s);
    }

    public BigTableException(Exception e){
        super(e);
    }
}
