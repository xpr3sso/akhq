import React from 'react';
import Header from '../../Header/Header';
import Form from '../../../components/Form/Form';
import {
    formatDateTime,
    transformStringArrayToViewOptions
} from '../../../utils/converters';
import Joi from 'joi-browser';
import {
    uriTopicExport,
    uriTopicsInfo,
    uriTopicsName,
    uriTopicsOffsetsByTimestamp
} from '../../../utils/endpoints';
import './styles.scss';
import {toast} from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import Dropdown from "react-bootstrap/Dropdown";
import DatePicker from "../../../components/DatePicker";
import moment from "moment";
import Input from "../../../components/Form/Input";

class TopicExport extends Form {
  state = {
    clusterId: '',
    topicId: '',
    selectedTopic: undefined,
    checked: {},
    formData: {
        lastMessagesNr:'',
        exportMethod: 'fullRecord',
        kvSep: ':',
        exportNulls: false
    },
    errors: {},
    loading: false,
    percent:0
  };

  schema = {
    lastMessagesNr: Joi.string()
      .allow('')
      .allow(null)
      .label('lastMessagesNr'),
    exportMethod: Joi.string()
      .label('exportMethod'),
    kvSep: Joi.string()
      .label('kvSep'),
    exportNulls: Joi.boolean()
      .label('exportNulls')
  };

  eventSource;
  formattedOutput;

  componentDidMount() {
    const { clusterId, topicId } = this.props.match.params;
    const { formData } = this.state;

    this.setState({ clusterId, topicId, formData }, () => {
      this.setupInitialData(clusterId, topicId);
    });
  }

  async setupInitialData(clusterId, topicId) {
    const { formData, checked } = this.state;

    const response = await this.getApi(uriTopicsInfo(clusterId, topicId));

    response.data.partitions.forEach(partition => {
          const name = `partition-${partition.id}`;
          const checkName = `check-${name}`;

          this.schema[name] = Joi.number()
              .min(partition.firstOffset || 0)
              .max(partition.lastOffset || 0)
              .required()
              .label(`Partition ${partition.id} offset`);

          formData[name] = partition.firstOffset || '0';
          checked[checkName] = true;
    });

    this.setState({
      formData,
      checked,
      selectedTopic: response.data,
      loading: false
    });
  }

  getTopics = (clusterId) => {
    this
        .getApi(uriTopicsName(clusterId))
        .then(res => {
          this.setState(
              { topics: transformStringArrayToViewOptions(res.data), loading: false }
          );
        })
        .catch(err => {
          console.error('Error:', err);
        });
  };

  createPartitionOffsetsList = (formData, checked) => {
    let partitionOffsetsList = [];
    let splitName = [];
    let partition = '';
    const checkedPartition= [];

    Object.keys(checked).forEach(checkedName => {
        splitName = checkedName.split('-');
        partition = splitName.pop();
        checkedPartition[partition] = checked[checkedName];
    });

    Object.keys(formData).filter(value => value.startsWith('partition')).forEach(name => {
      splitName = name.split('-');
      partition = splitName.pop();

      if(checkedPartition[partition] === true) {
          partitionOffsetsList.push(
              partition + "-" + formData[name]
          );
      }
    });

    return partitionOffsetsList.toString()
        .replaceAll(",", "_")
        .replace("[", "")
        .replace("]", "");
  };

  async doSubmit() {
    this.stopEventSource();
    this.startEventSource();
    toast.info(`Exporting started`);
  }

  startEventSource = () => {
    const { clusterId, topicId, formData, checked } = this.state;

    let self = this;
    let offsets = this.createPartitionOffsetsList(formData, checked);
    this.eventSource = new EventSource(uriTopicExport(clusterId, topicId, offsets));

    this.eventSource.addEventListener('searchBody', function(e) {
      const res = JSON.parse(e.data);
      const records = res.records || [];

      // self.setState({loading: true, percent: res.percent.toFixed(2) });
      self.handleMessages(records);
    });

    this.eventSource.addEventListener('searchEnd', function(e) {
      self.eventSource.close();
      self.setState({ loading: false, percent: 100});
      if (self.formattedOutput) {
          let blob = new Blob([self.formattedOutput], {type: 'text/plain'})
          toast.success(`Successfully exported from topic '${topicId}'`);
          const href = window.URL.createObjectURL(blob);
          const a = document.createElement('a');
          a.download = topicId.replaceAll(".", "_") + "_export.txt"
          a.href = href;
          a.click();
          a.href = '';
          self.props.history.push({
              pathname: `/ui/${clusterId}/topic/${topicId}`,
          });
      } else {
        toast.info(`Export from topic '${topicId}' returned empty!`);
      }
    });
  }

  stopEventSource = () => {
    if (this.eventSource) {
      this.eventSource.close();
    }
  };

  handleMessages = (records) => {
    const { formData } = this.state;
    records.forEach(record => {
      if(!formData.exportNulls && (record.value == null || record.value == "null")){
        return;
      }
      if(this.formattedOutput) {
        this.formattedOutput = this.formattedOutput + "\n" + this.formatOutput(record);
      } else {
        this.formattedOutput = this.formatOutput(record);
      }
    });
  };

  formatOutput = (message) => {
    const { formData } = this.state;
    switch (formData.exportMethod) {
        case "fullRecord":
            return JSON.stringify(message);
        case "keyValue":
            return (message.key ? message.key : "null") + formData.kvSep + message.value.replace(/[\r\n]+/gm,"");
        case "valueOnly":
            return message.value.replace(/[\r\n]+/gm, "");
    }
}

  checkedTopicOffset = (event) => {
    const { checked } = this.state;
    checked[event.target.value] = event.target.checked;

    this.setState({ checked: checked });
  }

  renderTopicPartition = () => {
    const { selectedTopic } = this.state;
    const renderedItems = [];

    if(selectedTopic) {

      renderedItems.push(
          <fieldset id={`fieldset-${selectedTopic.name}`} key={selectedTopic.name}>
            <legend id={`legend-${selectedTopic.name}`}>Partitions</legend>
            {this.renderPartitionInputs(selectedTopic.partitions)}
          </fieldset>
      );
    }
    return renderedItems;
  };

  renderPartitionInputs = (partitions) => {
    const { checked } = this.state;
    const renderedInputs = [];

    partitions.forEach(partition => {
      const name = `partition-${partition.id}`;
      const checkName = `check-${name}`;

      renderedInputs.push(
        <div className="form-group row row-checkbox" key={name}>
            { <input
                type="checkbox"
                value={checkName}
                checked={checked[checkName] || false}
                onChange={this.checkedTopicOffset}/>

            }
          <div className="col-sm-10 partition-input">
            <span id={`partition-${partition.id}-input`}>

              {this.renderInput(
                name,
                `Partition: ${partition.id}`,
                'Offset',
                'number',
                undefined,
                true,
                'partition-input-div',
                `partition-input ${name}-input col-auto`
              )}
            </span>
          </div>
        </div>
      );
    });

    return renderedInputs;
  };

  unCheckAll = (value)  => {
    const {checked} = this.state;

    Object.keys(checked).forEach(name => {
        checked[name] = value;
    });

    this.setState({ checked});
  }

  resetToFirstOffsets = () => {
    const { selectedTopic, formData } = this.state;

    selectedTopic.partitions.forEach(partition => {
      const name = `partition-${partition.id}`;
      formData[name] = partition.firstOffset || '0';
    });

    this.setState({ formData });
  };

  resetToLastOffsets = () => {
    const { selectedTopic, formData } = this.state;

    selectedTopic.partitions.forEach(partition => {
        const name = `partition-${partition.id}`;
        if(partition.firstOffset === partition.lastOffset) {
            formData[name] = partition.lastOffset || '0';
        } else if(partition.lastOffset > 0) {
             // Reduce last offset by one, otherwise no records would be fetched for export
            formData[name] = partition.lastOffset - 1;
        } else {
            formData[name] = '0';
        }
    });

    this.setState({ formData });
  };

  resetToCalculatedOffsets = ({ currentTarget: input }) => {
    const { selectedTopic, formData } = this.state;

    selectedTopic.partitions.forEach(partition => {
        const name = `partition-${partition.id}`;
        const calculatedOffset = (partition.lastOffset || 0) - input.value;
        formData[name] = (!calculatedOffset || calculatedOffset < 0 )? '0' : calculatedOffset;
    });

    formData['lastMessagesNr'] = input.value;
    this.setState({ formData });
  };

  async getTopicOffset() {
    const { clusterId, topicId, timestamp} = this.state;
    const momentValue = moment(timestamp);

    const date =
        timestamp.toString().length > 0
            ? formatDateTime(
            {
                year: momentValue.year(),
                monthValue: momentValue.month(),
                dayOfMonth: momentValue.date(),
                hour: momentValue.hour(),
                minute: momentValue.minute(),
                second: momentValue.second(),
                milli: momentValue.millisecond()
            },
            'YYYY-MM-DDTHH:mm:ss.SSS'
        ) + 'Z'
            : '';

    let data = {};
    if (date !== '') {
        data = await this.getApi(uriTopicsOffsetsByTimestamp(clusterId, topicId, date));
        data = data.data;
        this.handleOffsetsByTimestamp(data);
    }
  }

  handleOffsetsByTimestamp = partitions => {
    const { formData } = this.state;
    partitions.forEach(partition => {
        const name = `partition-${partition.partition}`;
        formData[name] = partition.offset || '0';
    });
    this.setState({ formData });
  };

  renderOptionButtons = () => {
    const { timestamp, formData} = this.state;
    const { loading } = this.props.history.location;

    return (
        <span>

    <div
        className="btn btn-secondary"
        type="button"
        style={{ marginRight: '0.5rem' }}
        onClick={() => this.unCheckAll(true)}
    >
      Check all partitions
    </div>
    <div
        className="btn btn-secondary"
        type="button"
        style={{ marginRight: '0.5rem' }}
        onClick={() => this.unCheckAll(false)}
    >
      Uncheck all partitions
    </div>
    <div
        className="btn btn-secondary"
        type="button"
        style={{ marginRight: '0.5rem' }}
        onClick={() => this.resetToFirstOffsets()}
    >
      Reset to first offsets
    </div>
    <div
        className="btn btn-secondary"
        type="button"
        style={{ marginRight: '0.5rem' }}
        onClick={() => this.resetToLastOffsets()}
    >
      Reset to last offsets
    </div>
    <div
        className="btn btn-secondary"
        type="button"
        style={{ marginRight: '0.5rem', padding: 0 }}
    >
      <Dropdown>
        <Dropdown.Toggle>Filter datetime</Dropdown.Toggle>
          {!loading && (
              <Dropdown.Menu>
                  <div>
                      <DatePicker
                          showTimeInput
                          showDateTimeInput
                          value={timestamp}
                          label={''}
                          onChange={value => {
                              this.setState({ timestamp: value }, () => this.getTopicOffset());
                          }}
                      />
                  </div>
              </Dropdown.Menu>
          )}
      </Dropdown>

    </div>

    <div
        className="btn btn-secondary"
        type="button"
        style={{ marginRight: '0.5rem', padding: 0 }}
    >
      <Dropdown>
        <Dropdown.Toggle>Last x messages per partition</Dropdown.Toggle>
          {!loading && (
              <Dropdown.Menu>
                  <div>
                      <Input
                          type='number'
                          name='lastMessagesNr'
                          id='lastMessagesNr'
                          value={formData['lastMessagesNr'] || ''}
                          label=''
                          placeholder='Last x messages'
                          onChange={this.resetToCalculatedOffsets}
                          noStyle=''
                          wrapperClass='input-nr-messages'
                          inputClass=''
                      />
                  </div>
              </Dropdown.Menu>
          )}
      </Dropdown>

    </div>
  </span>
    );
  };


  render() {
    const { topicId, formData, loading, percent} = this.state;

    return (
      <div>
        <Header title={`Export from ${topicId}`} history={this.props.history} />
        <form
          className="khq-form khq-export-topic"
          onSubmit={() => this.handleSubmit()}>

          {this.renderTopicPartition()}

          <fieldset id="cluster" key="cluster">
              <legend id="options">Export options</legend>

              <div className="row export-method">
                  <label className="col-sm-2 col-form-label">Export method</label>
                      {this.renderSelect(
                          'exportMethod',
                          '',
                          [{ name: "Full record", _id: "fullRecord" },
                          { name: "Key and Value", _id: "keyValue" },
                          { name: "Value only", _id: "valueOnly" }],
                          value => this.setState({
                                formData: {
                                    ...formData,
                                    exportMethod: value.target.value}
                                    }),
                          'col-auto',
                          undefined,
                          undefined
                          )}
              </div>
              {formData.exportMethod==="keyValue" && (
                  <div className="row kv-separator">
                      <label className="col-sm-2 col-form-label">Key-Value Separator</label>
                          {this.renderSelect(
                              'keyValueSeparator',
                              '',
                              [{ name: ":", _id: ":" },
                              { name: ".", _id: "." },
                              { name: ",", _id: "," },
                              { name: ";", _id: ";" },
                              { name: "-", _id: "-" },
                              { name: "_", _id: "_" }],
                              value => this.setState({
                                    formData: {
                                        ...formData,
                                        kvSep: value.target.value}
                                        }),
                              'col-auto'
                              )}
                  </div>
              )}

              <div className="row export-nulls">
                <label className="col-sm-2 col-form-label">Export null values</label>
                    { <input
                        type="checkbox"
                        value="exportNulls"
                        checked={formData.exportNulls}
                        onChange={
                            event => this.setState({
                                  formData: {
                                      ...formData,
                                      exportNulls: event.target.checked}})}
                      />
                    }
              </div>

          </fieldset>
            
            {this.renderButton(
                'Export',
                this.handleSubmit,
                undefined,
                'submit',
                this.renderOptionButtons()
            )}
        </form>
      </div>
    );
  }
}

export default TopicExport;
