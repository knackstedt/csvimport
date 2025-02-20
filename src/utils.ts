import fs from 'fs';

export const countNewlines = (csvFilename: string) => {
    const fileStream = fs.createReadStream('./data/' + csvFilename, { highWaterMark: 512 * 1024 });
    let count = 0;
    const td = new TextDecoder();
    return new Promise<number>((res, rej) => {
        fileStream.on("data", data => {
            const text = td.decode(data as Buffer);
            count += text.match(/\n/g).length;
        })
        fileStream.on("end", () => {
            res(count);
        })
    })
}

export const printTime = (duration: number) => {
    let milliseconds = Math.floor((duration % 1000) / 100);
    let seconds = Math.floor((duration / 1000) % 60);
    let minutes = Math.floor((duration / (1000 * 60)) % 60);
    let hours = Math.floor((duration / (1000 * 60 * 60)) % 24);

    hours = (hours < 10) ? 0 + hours : hours;
    minutes = (minutes < 10) ? 0 + minutes : minutes;
    seconds = (seconds < 10) ? 0 + seconds : seconds;

    return (hours ? (hours + "h ") : '') + minutes + "m " + seconds + "." + milliseconds + "s";
}
