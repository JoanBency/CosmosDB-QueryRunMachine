const winston = require('winston');
const winstonRotator = require('winston-daily-rotate-file');

const logConfiguration = {
  transports: [
    new (winston.transports.DailyRotateFile)({
      name: 'DMLQueryStats',
      level: 'info',
      filename: './Documents/DMLQueryStats',
      json: false,
    //   datePattern: 'yyyy-MM-DD',
      prepend: true,
      maxFiles: '5m'
    })
  ],
  format: winston.format.combine(
      winston.format.timestamp({
         format: 'YYYY-MM-DD HH:mm:ss:ms'
     }),
      winston.format.printf(info => `${[info.timestamp]},${info.message}`),
  )
};

const logger = winston.createLogger(logConfiguration);


module.exports = { logger };