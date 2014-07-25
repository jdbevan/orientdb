package com.orientechnologies.orient.core.record.impl;

import java.util.Arrays;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinary;

public class CachedStringSerializationTest {

  private ODatabaseDocument databaseDocument;
  private ORecordSerializer serializerPre;

  @BeforeTest
  public void before() {
    serializerPre = ODatabaseDocumentTx.getDefaultSerializer();
    ODatabaseDocumentTx.setDefaultSerializer(ORecordSerializerFactory.instance().getFormat(ORecordSerializerBinary.NAME));
    databaseDocument = new ODatabaseDocumentTx("memory:" + CachedStringSerializationTest.class.getSimpleName()).create();
  }
 
  @AfterTest
  public void after() {
    ODatabaseDocumentTx.setDefaultSerializer(serializerPre);
    databaseDocument.drop();
  }

  @Test
  public void testCachedSerialization() {
    ODatabaseRecordThreadLocal.INSTANCE.set(databaseDocument);
    ODocument doc = new ODocument();
    doc.field("test", "aTest");
    doc.field("test1", 10);
    byte[] res = doc.toStream();
    databaseDocument.getMetadata().getSchema().addCachedName("test");
    databaseDocument.getMetadata().getSchema().addCachedName("test1");
    doc.setDirty();
    byte[] res2 = doc.toStream();
    Assert.assertFalse(Arrays.equals(res, res2));
    ODocument doc2 = new ODocument();
    doc2.fromStream(res2);
    Assert.assertEquals(doc2.field("test"), doc.field("test"));
    Assert.assertEquals(doc2.field("test1"), doc.field("test1"));
  }
}
