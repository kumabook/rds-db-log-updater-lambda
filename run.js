const { handler } = require('./index');

handler().then((res) => {
  console.log(res);
}, (err) => {
  console.log(err);
});
