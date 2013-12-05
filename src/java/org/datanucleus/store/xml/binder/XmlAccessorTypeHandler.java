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
    ...
 **********************************************************************/
package org.datanucleus.store.xml.binder;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;

import org.datanucleus.metadata.AbstractClassMetaData;

public class XmlAccessorTypeHandler implements InvocationHandler
{
    private XmlAccessorTypeHandler()
    {
    }

    public static Annotation newProxy(AbstractClassMetaData acmd)
    {
       return (Annotation)Proxy.newProxyInstance(AbstractClassMetaData.class.getClassLoader(),
         new Class[]{XmlAccessorType.class}, new XmlAccessorTypeHandler());
    }

   public Object invoke(Object proxy, Method method, Object[] args) throws Throwable
   {
       String name = method.getName();

       if (name.equals("annotationType"))
       {
           return XmlAccessorType.class;
       }
       else if (name.equals("value"))
       {
           // Use every non-static non-transient field for serialisation
           // TODO Allow capability to use properties "PROPERTY"
           return XmlAccessType.valueOf("FIELD");
       }
       return null;
   }
}