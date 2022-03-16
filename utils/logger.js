const winston = require('winston');
const winstonRotator = require('winston-daily-rotate-file');

const logConfiguration = {
  transports: [
    new (winston.transports.DailyRotateFile)({
      name: 'access-file',
      level: 'info',
      filename: './logs/access.log',
      json: false,
      datePattern: 'yyyy-MM-DD',
      prepend: true,
      maxFiles: '5m'
    })
  ],
  format: winston.format.combine(
      winston.format.label({
          label: `Log`
      }),
      winston.format.timestamp({
         format: 'MMM-DD-YYYY HH:mm:ss:ms'
     }),
      winston.format.printf(info => `${[info.timestamp]}: ${info.message}`),
  )
};

const logger = winston.createLogger(logConfiguration);


module.exports = { logger };



