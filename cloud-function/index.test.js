const expect = require('chai').expect
const index = require('./index')
const google = require('googleapis');

describe('The cloud function', function () {
  it('Check eventType & file path', function * () {

      const event = {
        data: {
          bucket: "gs://batch-pipeline",
          name: "upload/filename"
        },
        context: {
          eventType: "google.storage.object.finalize"
        }
      };

    const getApplicationDefaultStub = this.sandbox.stub(google.auth, 'getApplicationDefault');

    // Call tested function and verify its behavior
    const result = index.goWithTheDataFlow(event, () => {
      t.end();
    });

    expect(getApplicationDefaultStub).to.be.calledWith()
  })
})