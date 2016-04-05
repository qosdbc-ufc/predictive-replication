/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.provider.api;

/**
 *
 * @author Leonardo Oliveira Moreira
 *
 * Class to represent the exceptions of this API
 */
public class ProviderException extends Throwable {

    public ProviderException(Throwable cause) {
        super(cause);
    }

    public ProviderException(String message) {
        super(message);
    }

    public ProviderException(String message, Throwable cause) {
        super(message, cause);
    }
}