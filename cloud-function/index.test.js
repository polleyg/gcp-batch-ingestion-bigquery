const expect = require('chai').expect
const index = require('./index')

describe('The cloud function', function () {
  it('GCS trigger', function * () {

      const event = {
        data: {
          bucket: "gs://batch-pipeline",
          name: "upload/filename"
        },
        context: {
          eventType: "google.storage.object.finalize"
        }
      };

    // Call tested function and verify its behavior
    const result = index.goWithTheDataFlow(event, () => {
      t.end();
    });

  })
})