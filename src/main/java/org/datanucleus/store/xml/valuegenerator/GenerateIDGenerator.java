/**********************************************************************
Copyright (c) 2010 Erik Bengtson and others. All rights reserved.
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
    ...
**********************************************************************/
package org.datanucleus.store.xml.valuegenerator;

import java.util.Properties;

import javax.xml.xpath.XPathFactory;

import org.datanucleus.store.StoreManager;
import org.datanucleus.store.valuegenerator.AbstractConnectedGenerator;
import org.datanucleus.store.valuegenerator.ValueGenerationBlock;
import org.datanucleus.store.valuegenerator.ValueGenerationException;

/**
 * Value generator for calling xpath generate-id().
 */
public class GenerateIDGenerator extends AbstractConnectedGenerator<String>
{
    /**
     * Constructor.
     * @param storeMgr StoreManager
     * @param name Symbolic name of the generator
     * @param props Any properties controlling its behaviour.
     */
    public GenerateIDGenerator(StoreManager storeMgr, String name, Properties props)
    {
        super(storeMgr, name, props);
    }

    /**
     * Method to reserve a block of values.
     * Only ever reserves a single timestamp, to the time at which it is created.
     * @param size Number of elements to reserve.
     * @return The block.
     */
    protected ValueGenerationBlock<String> reserveBlock(long size)
    {
        try
        {
            //TODO must provide the node, and not the root. otherwise the id is always generated with same value
            Object doc = connectionProvider.retrieveConnection().getConnection();
            String id = XPathFactory.newInstance().newXPath().evaluate("generate-id(.)", doc);
            return new ValueGenerationBlock<String>(new String[]{id});
        }
        catch (Exception e)
        {
            throw new ValueGenerationException(e.getMessage(), e);
        }
        finally
        {
            connectionProvider.releaseConnection();
        }
    }
}