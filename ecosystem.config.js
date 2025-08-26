module.exports = {
  apps: [
    {
      name: 'kristina-bot',
      script: 'index.js',
      instances: 1,
      autorestart: true,
      watch: false,
      env: {
        NODE_ENV: 'production',
        BOT_INSTANCE: 'kristina'
      },
      error_file: './logs/kristina-error.log',
      out_file: './logs/kristina-out.log',
      log_file: './logs/kristina-combined.log',
      time: false
    },
    {
      name: 'dispatcher1-bot',
      script: 'index.js',
      instances: 1,
      autorestart: true,
      watch: false,
      env: {
        NODE_ENV: 'production',
        BOT_INSTANCE: 'dispatcher1'
      },
      error_file: './logs/dispatcher1-error.log',
      out_file: './logs/dispatcher1-out.log',
      log_file: './logs/dispatcher1-combined.log',
      time: false
    },
    {
      name: 'dispatcher2-bot',
      script: 'index.js',
      instances: 1,
      autorestart: true,
      watch: false,
      env: {
        NODE_ENV: 'production',
        BOT_INSTANCE: 'dispatcher2'
      },
      error_file: './logs/dispatcher2-error.log',
      out_file: './logs/dispatcher2-out.log',
      log_file: './logs/dispatcher2-combined.log',
      time: false
    },
    {
      name: 'dispatcher3-bot',
      script: 'index.js',
      instances: 1,
      autorestart: true,
      watch: false,
      env: {
        NODE_ENV: 'production',
        BOT_INSTANCE: 'dispatcher3'
      },
      error_file: './logs/dispatcher3-error.log',
      out_file: './logs/dispatcher3-out.log',
      log_file: './logs/dispatcher3-combined.log',
      time: false
    }
  ]
};