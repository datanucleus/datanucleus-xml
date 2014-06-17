/**********************************************************************
Copyright (c) 2008 Erik Bengtson and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors:
2008 Andy Jefferson - rework file opening/closing, and use of managed resource
    ...
**********************************************************************/
package org.datanucleus.store.xml;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;

import javax.transaction.xa.XAResource;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.datanucleus.ExecutionContext;
import org.datanucleus.PropertyNames;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.AbstractConnectionFactory;
import org.datanucleus.store.connection.AbstractManagedConnection;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.util.NucleusLogger;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

/**
 * Implementation of a ConnectionFactory for XML.
 * The connectionUrl defines the XML filename to be used. Also support persistence property "datanucleus.xml.indentSize" to control
 * the indenting used in the XML file.
 */
public class ConnectionFactoryImpl extends AbstractConnectionFactory
{
    String filename = null;

    int indent = 4;

    /**
     * Constructor.
     * @param storeMgr Store Manager
     * @param resourceType Type of resource (tx, nontx)
     */
    public ConnectionFactoryImpl(StoreManager storeMgr, String resourceType)
    {
        super(storeMgr, resourceType);

        // "xml:file:{filename}"
        String url = storeMgr.getConnectionURL();
        if (url == null)
        {
            throw new NucleusException("You haven't specified persistence property '" + PropertyNames.PROPERTY_CONNECTION_URL + "' (or alias)");
        }
        if (!url.startsWith("xml"))
        {
            throw new NucleusException("invalid URL: "+url);
        }

        // Split the URL into filename
        String str = url.substring(4); // Omit the prefix
        if (str.indexOf("file:") != 0)
        {
            throw new NucleusException("invalid URL: "+url);
        }

        filename = str.substring(5);

        indent = storeMgr.getIntProperty(XMLStoreManager.XML_INDENT_SIZE_PROPERTY);
    }

    /**
     * Obtain a connection from the Factory. The connection will be enlisted within the transaction
     * associated to the ExecutionContext
     * @param ec the pool that is bound the connection during its lifecycle (or null)
     * @param options Options for creating the connection
     * @return the {@link org.datanucleus.store.connection.ManagedConnection}
     */
    public ManagedConnection createManagedConnection(ExecutionContext ec, Map options)
    {
        return new ManagedConnectionImpl();
    }

    public class ManagedConnectionImpl extends AbstractManagedConnection
    {
        /** The XML File. */
        File file;
        
        public ManagedConnectionImpl()
        {
        }

        public Object getConnection()
        {
            if (conn == null)
            {
                try
                {
                    file = new File(filename);
                    if (!file.exists())
                    {
                        file.createNewFile();
                        conn = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
                    }
                    else
                    {
                        try
                        {
                            // TODO This can cause System.out messages like "[Fatal Error] test.xml:3:1: Premature end of file." if not valid XML
                            // e.g a single line file like "<?xml version="1.0" encoding="UTF-8" standalone="no"?>"
                            conn = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(file);
                        }
                        catch (SAXException ex)
                        {
                            conn = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
                        }
                    }
                }
                catch (IOException e)
                {
                    NucleusLogger.CONNECTION.error("Exception getting connection to XML file", e);
                    throw new NucleusException(e.getMessage(), e);
                }
                catch (ParserConfigurationException e)
                {
                    NucleusLogger.CONNECTION.error("Exception getting connection to XML file", e);
                    throw new NucleusException(e.getMessage(), e);
                }
            }
            return conn;
        }

        public void release()
        {
            if (commitOnRelease)
            {
                try
                {
                    DOMSource source = new DOMSource((Document)conn);
                    FileOutputStream os = new FileOutputStream(file);
                    StreamResult result = new StreamResult(os);
                    Transformer t = getTransformer();
                    t.transform(source, result);
                    os.close();
                    conn = null;
                }
                catch (Exception e)
                {
                    NucleusLogger.CONNECTION.error("Exception closing connection to XML file", e);
                    throw new NucleusException(e.getMessage(),e);
                }
            }
            super.release();
        }

        public void close()
        {
            if (conn == null)
            {
                return;
            }

            for (int i=0; i<listeners.size(); i++)
            {
                listeners.get(i).managedConnectionPreClose();
            }
            try
            {
                try
                {
                    DOMSource source = new DOMSource((Document)conn);
                    FileOutputStream os = new FileOutputStream(file);
                    StreamResult result = new StreamResult(os);
                    Transformer t = getTransformer();
                    t.transform(source, result);
                    os.close();
                }
                catch (Exception e)
                {
                    NucleusLogger.CONNECTION.error("Exception closing connection to XML file", e);
                    throw new NucleusException(e.getMessage(),e);
                }
            }
            finally
            {
                conn = null;
                for (int i=0; i<listeners.size(); i++)
                {
                    listeners.get(i).managedConnectionPostClose();
                }
            }
        }

        private Transformer getTransformer() throws TransformerConfigurationException
        {
            TransformerFactory tf = TransformerFactory.newInstance();
            try
            {
                tf.setAttribute("indent-number", indent); // JDK-repackaged Xalan supports this
            }
            catch (IllegalArgumentException iae)
            {}

            Transformer t = tf.newTransformer();
            t.setOutputProperty(OutputKeys.INDENT, "yes");
            t.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "" + indent);

            return t;
        }

        public XAResource getXAResource()
        {
            return null;
        }
    }
}
