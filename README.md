# Logstash output plugin for Site24x7

With Site24x7 plugin for Logstash, you can parse and send logs directly from Logstash, without having to use a separate log shipper.

# Installation

To add the plugin to your Logstash, use the following command:

```
logstash-plugin install logstash-output-site24x7
```

## Usage

**Configure the output plugin**

To forward events to Site24x7 add the following code to your Logstash configuration file
```
output {
    site24x7 {
        log_type_config => "<your_log_type_config>"
    }
}
```
## Parameters

Property | Description | Default Value
------------ | -------------|------------
log_type_config | log_type_config of your configured log type in site24x7 | nil
max_retry | Number of times to resend failed uploads | 3
retry_interval |  Time interval to sleep initially between retries, exponential step-off | 2 seconds
## Need Help?

If you need any support please contact us at support@site24x7.com.
