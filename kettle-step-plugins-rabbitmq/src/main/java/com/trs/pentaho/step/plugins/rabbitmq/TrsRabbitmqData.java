package com.trs.pentaho.step.plugins.rabbitmq;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class TrsRabbitmqData extends BaseStepData implements StepDataInterface {

    public RowMetaInterface inputRowMeta;
    public org.pentaho.di.core.row.RowMetaInterface outputRowMeta;

    public TrsRabbitmqData() {
        super();
    }

}
