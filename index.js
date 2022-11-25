const {google} = require('googleapis');
const vision = require('@google-cloud/vision');

const {CLOUD_RUN_TASK_INDEX = 0, CLOUD_RUN_TASK_ATTEMPT = 0} = process.env;
const {INBOX, OUTBOX} = process.env;

const auth = new google.auth.GoogleAuth({
    scopes: ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/cloud-vision']
});

const main = async () => {
    console.log(`Starting Task #${CLOUD_RUN_TASK_INDEX}, Attempt #${CLOUD_RUN_TASK_ATTEMPT}...`);

    const drive = google.drive({version: 'v3', auth});

    const files = await listFiles(drive);
    files.map(async (file) => {
        const content = await retrieveContent(drive, file);
        const annotation = await annotateImage(content);
        if (annotation) {
            await saveAnnotation(drive, file, annotation);
            await moveFile(drive, file);
        }
    });
};

async function listFiles(drive) {
    const res = await drive.files.list({
        q: `'${INBOX}' in parents`,
        fields: 'files(id, name)',
    });
    return res.data.files;
}

async function retrieveContent(drive, file) {
    return await drive.files.get({fileId: file.id, alt: 'media'}, {responseType: "stream"})
        .then(res => {
            return new Promise((resolve, reject) => {
                let buffer = [];
                res.data.on("data", d => buffer.push(d));
                res.data.on("end", () => {
                    resolve(Buffer.concat(buffer).toString('base64'));
                });
                res.data.on('error', err => {
                    console.error('Error downloading file.');
                    reject(err);
                })
            });
        });
}

async function annotateImage(content) {
    const client = new vision.ImageAnnotatorClient({auth});
    const request = {
        image: {
            content,
        },
        features: [{type: 'TEXT_DETECTION'}],
    };

    const [result] = await client.annotateImage(request);
    const annotations = result.textAnnotations;
    if (annotations && annotations.length > 0) {
        return annotations[0].description;
    } else {
        return null;
    }
}

async function saveAnnotation(drive, file, annotation) {
    const fileMetadata = {
        'name': file.name + '.txt',
        parents: [OUTBOX]
    };
    const media = {
        mimeType: 'text/plain',
        body: annotation
    };

    return await drive.files.create({
        resource: fileMetadata,
        media: media,
        fields: 'id'
    }, (err) => {
        if (err) {
            console.error(err);
        }
    })
}

async function moveFile(drive, file) {
    return await drive.files.update({
        fileId: file.id,
        addParents: OUTBOX,
        removeParents: INBOX,
        fields: 'id, parents'
    }, function (err) {
        if (err) {
            console.error(err);
        }
    });
}

main().catch(err => {
    console.error(err);
    process.exit(1);
});
