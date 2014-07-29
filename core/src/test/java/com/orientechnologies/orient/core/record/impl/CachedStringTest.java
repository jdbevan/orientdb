package com.orientechnologies.orient.core.record.impl;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinary;

public class CachedStringTest {

  private ODatabaseDocument databaseDocument;
  private boolean           preStorageKeepOpen;

  @BeforeTest
  public void before() {
    ODatabaseDocumentTx.setDefaultSerializer(ORecordSerializerFactory.instance().getFormat(ORecordSerializerBinary.NAME));
    databaseDocument = new ODatabaseDocumentTx("memory:" + CachedStringTest.class.getSimpleName()).create();
    preStorageKeepOpen = OGlobalConfiguration.STORAGE_KEEP_OPEN.getValueAsBoolean();
    OGlobalConfiguration.STORAGE_KEEP_OPEN.setValue(false);
  }

  @AfterTest
  public void after() {
    OGlobalConfiguration.STORAGE_KEEP_OPEN.setValue(preStorageKeepOpen);
    databaseDocument.drop();
  }

  @Test
  public void testCachedString() {
    ODatabaseRecordThreadLocal.INSTANCE.set(databaseDocument);
    OSchema schema = databaseDocument.getMetadata().getSchema();
    int pos = schema.addCachedName("test");
    Assert.assertEquals(schema.addCachedName("test"), pos);
    Assert.assertEquals(schema.getCachedNameId("test"), pos);
    Assert.assertEquals(schema.getCachedNameById(pos), "test");
    databaseDocument.close();
    databaseDocument.open("admin", "admin");
    OSchema schema1 = databaseDocument.getMetadata().getSchema();

    Assert.assertEquals(schema1.getCachedNameId("test"), pos);
    Assert.assertEquals(schema1.getCachedNameById(pos), "test");

  }

}
