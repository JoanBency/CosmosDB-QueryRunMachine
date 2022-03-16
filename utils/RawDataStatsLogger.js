const winston = require('winston');
const winstonRotator = require('winston-daily-rotate-file');

const logConfiguration = {
  transports: [
    new (winston.transports.DailyRotateFile)({
      name: 'RawDataStats',
      level: 'info',
      filename: './logs/RawDataStats',
      json: false,
    //   datePattern: 'yyyy-MM-DD',
      prepend: true,
      maxFiles: '100m'
    })
  ],
  format: winston.format.combine(
      winston.format.printf(info => `${info.message}`),
  )
};

const logger = winston.createLogger(logConfiguration);


module.exports = { logger };