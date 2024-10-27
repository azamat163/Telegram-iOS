//
//  CustomHLSPlayer.swift
//  Telegram
//
//  Created by Азамат Агатаев on 26.10.2024.
//

import Foundation
import AVFoundation
import QuartzCore
import UIKit
import VideoToolbox

enum VideoQuality {
    case auto
    case quality(bitrate: Int)
}

enum CustomHLSPlayerActionAtItemEnd {
    case pause
    case stop
    case loop
}

private let decompressionOutputCallback: VTDecompressionOutputCallback = { (
    decompressionOutputRefCon,
    sourceFrameRefCon,
    status,
    infoFlags,
    imageBuffer,
    presentationTimeStamp,
    duration
) in
    guard let imageBuffer = imageBuffer, status == noErr else {
        print("Ошибка декодирования кадра: \(status)")
        return
    }

    let selfInstance = Unmanaged<CustomHLSPlayer>.fromOpaque(decompressionOutputRefCon!).takeUnretainedValue()
    
    DispatchQueue.main.async {
        selfInstance.displayLayer?.displayImageBuffer(imageBuffer)
    }
}

final class CustomHLSPlayer: NSObject {
    private let parser = HLSParser()
    var displayLayer: CustomHLSPlayerLayer?
    private var videoSession: VTDecompressionSession?
    
    private let audioEngine = AVAudioEngine()
    private let audioPlayerNode = AVAudioPlayerNode()
    private var audioFormat: AVAudioFormat?
    
    private var successfulReads: Int = 0
    private var failedReads: Int = 0
    private let requiredBufferForFullStatus = 5
    private let maxFailedReadsForEmptyBuffer = 3
    private var currentSegmentIndex = 0
    private var isSendingFrames = true
    
    private var lastBufferCheckTime: Date = Date()
    private var bufferDuration: Double = 0.0
    
    var rate: Float = 1.0
    var defaultRate: Float = 1.0
    var volume: Float = 1.0 {
        didSet {
            audioPlayerNode.volume = volume
        }
    }
    var actionAtItemEnd: CustomHLSPlayerActionAtItemEnd = .pause
    
    private(set) var isPlaying = false
    private var currentTimeInternal: CMTime = .zero
    private var currentItemInternal: CustomHLSPlayerItem?
    private var availableQualities: [URL] = []
    private(set) var currentQuality: VideoQuality = .auto
    
    var currentItem: CustomHLSPlayerItem? {
        return currentItemInternal
    }
    
    override init() {
        super.init()
        setupAudioEngine()
    }
    
    func load(from url: URL, preferredQualityIndex: Int = 0, completion: @escaping (Bool) -> Void) {
        parser.loadPlaylist(from: url) { [weak self] success in
            guard success, let self = self else {
                completion(false)
                return
            }
            self.currentSegmentIndex = 0
            self.loadSegment(at: self.currentSegmentIndex)
            completion(true)
        }
    }
    
    private func loadSegment(at index: Int) {
        guard index < parser.segments.count else { return }
        let segment = parser.segments[index]
        
        URLSession.shared.dataTask(with: segment.url) { [weak self] data, _, error in
            guard let data = data, error == nil, let self = self else { return }
            self.decodeSegment(data)
        }.resume()
    }
    
    private func playNextSegment() {
        guard isPlaying, currentSegmentIndex < parser.segments.count else { return }
        loadSegment(at: currentSegmentIndex)
        currentSegmentIndex += 1
    }
    
    private func decodeSegment(_ data: Data) {
        decodeVideoData(data)
        decodeAudioData(data)
    }
    
    private func decodeVideoData(_ data: Data) {
        data.withUnsafeBytes { bufferPointer in
            guard let baseAddress = bufferPointer.baseAddress else { return }
            
            var blockBuffer: CMBlockBuffer?
            
            let status = CMBlockBufferCreateWithMemoryBlock(
                allocator: kCFAllocatorDefault,
                memoryBlock: UnsafeMutableRawPointer(mutating: baseAddress),
                blockLength: data.count,
                blockAllocator: kCFAllocatorNull,
                customBlockSource: nil,
                offsetToData: 0,
                dataLength: data.count,
                flags: 0,
                blockBufferOut: &blockBuffer
            )
            
            guard status == kCMBlockBufferNoErr, let buffer = blockBuffer else {
                print("Ошибка создания CMBlockBuffer: \(status)")
                return
            }
            
            var sampleBuffer: CMSampleBuffer?
            let sampleBufferStatus = CMSampleBufferCreateReady(
                allocator: kCFAllocatorDefault,
                dataBuffer: buffer,
                formatDescription: nil,
                sampleCount: 1,
                sampleTimingEntryCount: 0,
                sampleTimingArray: nil,
                sampleSizeEntryCount: 1,
                sampleSizeArray: [data.count],
                sampleBufferOut: &sampleBuffer
            )
            
            guard sampleBufferStatus == noErr, let sampleBuffer = sampleBuffer else {
                print("Ошибка создания CMSampleBuffer: \(sampleBufferStatus)")
                return
            }
            
            guard let videoSession = videoSession else {
                print("Video session не инициализирован")
                return
            }
            
            let selfPointer = Unmanaged.passUnretained(self).toOpaque()
            
            let decodeStatus = VTDecompressionSessionDecodeFrame(
                videoSession,
                sampleBuffer: sampleBuffer,
                flags: [],
                frameRefcon: selfPointer,
                infoFlagsOut: nil
            )
            
            if decodeStatus != noErr {
                print("Ошибка декодирования в VTDecompressionSession: \(decodeStatus)")
            }
        }
    }
    
    private func decodeAudioData(_ data: Data) {
        let audioBuffer = AVAudioPCMBuffer(
            pcmFormat: audioPlayerNode.outputFormat(forBus: 0),
            frameCapacity: AVAudioFrameCount(data.count)
        )!
        
        audioBuffer.frameLength = audioBuffer.frameCapacity
        memcpy(audioBuffer.int16ChannelData![0], [UInt8](data), data.count)
        
        audioPlayerNode.scheduleBuffer(audioBuffer, at: nil, options: .interruptsAtLoop, completionHandler: nil)
    }
    
    func replaceCurrentItem(with item: CustomHLSPlayerItem?, preferredQualityIndex: Int = 0, completion: @escaping (Bool) -> Void) {
        guard let item else { return }
        item.loadPlaylist { [weak self] success in
            guard success, let self = self else {
                completion(false)
                return
            }
            
            self.currentItemInternal = item
            self.availableQualities = item.availableQualities
            
            if preferredQualityIndex < self.availableQualities.count {
                self.currentQuality = .quality(bitrate: preferredQualityIndex)
            } else {
                self.currentQuality = .auto
            }
            
            self.currentSegmentIndex = 0
            self.isPlaying = false
            self.loadDataForFirstSegment(item.segments[0]) { data in
                guard let data = data, let formatDescription = self.extractFormatDescription(from: data) else {
                    print("Не удалось получить формат описания из первого сегмента")
                    completion(false)
                    return
                }
                
                self.setupVideoSession(for: formatDescription)
                self.loadSegment(at: self.currentSegmentIndex)
                completion(true)
            }
        }
    }
    
    private func setupVideoSession(for formatDescription: CMFormatDescription) {
        var callbackRecord = VTDecompressionOutputCallbackRecord(
            decompressionOutputCallback: decompressionOutputCallback,
            decompressionOutputRefCon: Unmanaged.passUnretained(self).toOpaque()
        )
        
        let destinationPixelBufferAttributes: [NSString: Any] = [
            kCVPixelBufferPixelFormatTypeKey: kCVPixelFormatType_420YpCbCr8BiPlanarFullRange,
            kCVPixelBufferWidthKey: CMVideoFormatDescriptionGetDimensions(formatDescription).width,
            kCVPixelBufferHeightKey: CMVideoFormatDescriptionGetDimensions(formatDescription).height,
            kCVPixelBufferOpenGLCompatibilityKey: true
        ]
        
        var destinationAttributes: CFDictionary?
        destinationAttributes = destinationPixelBufferAttributes as CFDictionary
        
        var videoSession: VTDecompressionSession?
        let status = VTDecompressionSessionCreate(
            allocator: kCFAllocatorDefault,
            formatDescription: formatDescription,
            decoderSpecification: nil,
            imageBufferAttributes: destinationAttributes,
            outputCallback: &callbackRecord,
            decompressionSessionOut: &videoSession
        )
        
        guard status == noErr, let session = videoSession else {
            print("Ошибка создания VTDecompressionSession: \(status)")
            return
        }
        
        self.videoSession = session
        print("Успешное создание VTDecompressionSession")
    }
    
    private func loadDataForFirstSegment(_ segment: HLSParser.Segment, completion: @escaping (Data?) -> Void) {
        URLSession.shared.dataTask(with: segment.url) { data, _, error in
            if let error = error {
                print("Ошибка загрузки данных сегмента: \(error)")
                completion(nil)
            } else {
                completion(data)
            }
        }.resume()
    }
    
    private func extractFormatDescription(from segment: Data) -> CMFormatDescription? {
        var formatDescription: CMFormatDescription?
        
        let startCode: [UInt8] = [0, 0, 0, 1]
        
        guard let spsRange = segment.range(of: Data(startCode) + [0x67]),
              let ppsRange = segment.range(of: Data(startCode) + [0x68])
        else {
            print("Ошибка: не удалось найти SPS и PPS в сегменте")
            return nil
        }
        
        let spsData = segment[spsRange.upperBound ..< ppsRange.lowerBound]
        let ppsData = segment[ppsRange.upperBound ..< segment.endIndex]
        
        return spsData.withUnsafeBytes { spsPointer in
            return ppsData.withUnsafeBytes { ppsPointer in
                guard let spsBaseAddress = spsPointer.baseAddress,
                      let ppsBaseAddress = ppsPointer.baseAddress else {
                    print("Ошибка: SPS или PPS данные не удалось получить")
                    return nil
                }
                
                let parameterSetPointers: [UnsafePointer<UInt8>] = [
                    spsBaseAddress.assumingMemoryBound(to: UInt8.self),
                    ppsBaseAddress.assumingMemoryBound(to: UInt8.self)
                ]
                
                let parameterSetSizes = [spsData.count, ppsData.count]
                
                let status = CMVideoFormatDescriptionCreateFromH264ParameterSets(
                    allocator: kCFAllocatorDefault,
                    parameterSetCount: parameterSetPointers.count,
                    parameterSetPointers: parameterSetPointers,
                    parameterSetSizes: parameterSetSizes,
                    nalUnitHeaderLength: 4,
                    formatDescriptionOut: &formatDescription
                )
                
                if status != noErr {
                    print("Ошибка создания CMFormatDescription: \(status)")
                    return nil
                }
                
                return formatDescription
            }
        }
    }

    
    func setLayer(_ layer: CustomHLSPlayerLayer) {
        self.displayLayer = layer
    }
    
    private func handleEndOfItem() {
        switch actionAtItemEnd {
        case .pause:
            pause()
        case .stop:
            stop()
        case .loop:
            seek(to: .zero)
            play()
        }
        
        currentItem?.notifyPlayToEnd()
    }
    
    func play() {
        guard !isPlaying else { return }
        isPlaying = true
        audioPlayerNode.play()
        isSendingFrames = true
        playNextSegment()
    }
    
    func pause() {
        guard isPlaying else { return }
        isPlaying = false
        audioPlayerNode.pause()
        isSendingFrames = false
    }
    
    func stop() {
        isPlaying = false
        currentSegmentIndex = 0
        isSendingFrames = false
        audioPlayerNode.stop()
        videoSession = nil
    }
    
    func currentTime() -> CMTime {
        return currentTimeInternal
    }
    
    func seek(to time: CMTime) {
        guard let item = currentItem else { return }
        
        let targetTimeInSeconds = CMTimeGetSeconds(time)
        var accumulatedTime: Double = 0.0
        var targetSegmentIndex = 0
        
        for (index, segment) in item.segments.enumerated() {
            accumulatedTime += segment.duration
            if accumulatedTime >= targetTimeInSeconds {
                targetSegmentIndex = index
                break
            }
        }
        
        currentSegmentIndex = targetSegmentIndex
        loadSegment(at: currentSegmentIndex)
        
        if isPlaying {
            play()
        }
    }

    
    func currentTimeInSeconds() -> Double {
        return currentTimeInternal.seconds
    }
    
    private func checkBufferStatus() {
        let currentTime = Date()
        let timeSinceLastCheck = currentTime.timeIntervalSince(lastBufferCheckTime)
        lastBufferCheckTime = currentTime
        
        let isBufferEmpty = failedReads >= maxFailedReadsForEmptyBuffer
        let likelyToKeepUp = successfulReads > failedReads
        let isBufferFull = bufferDuration >= Double(requiredBufferForFullStatus)
        
        currentItem?.updateBufferStatus(isBufferEmpty: isBufferEmpty, likelyToKeepUp: likelyToKeepUp, bufferFull: isBufferFull)
        
        if timeSinceLastCheck > 1.0 {
            bufferDuration = 0.0
            failedReads = 0
            successfulReads = 0
        }
    }
    
    private func setupAudioEngine() {
        audioEngine.attach(audioPlayerNode)
        audioEngine.connect(audioPlayerNode, to: audioEngine.mainMixerNode, format: nil)
        
        do {
            try audioEngine.start()
        } catch {
            print("Ошибка запуска аудио движка: \(error)")
        }
    }
}

extension CustomHLSPlayer {
    override class func automaticallyNotifiesObservers(forKey key: String) -> Bool {
        if key == "rate" {
            return true
        }
        return super.automaticallyNotifiesObservers(forKey: key)
    }
}

final class CustomHLSPlayerLayer: CALayer {
    weak var player: CustomHLSPlayer?
    
    init(player: CustomHLSPlayer?) {
        self.player = player
        super.init()
        setupLayer()
    }
    
    override init(layer: Any) {
        super.init(layer: layer)
    }
    
    required init?(coder: NSCoder) {
        super.init(coder: coder)
    }
    
    private func setupLayer() {
        self.contentsGravity = .resizeAspect
        self.backgroundColor = UIColor.black.cgColor
    }
    
    func displayImageBuffer(_ imageBuffer: CVImageBuffer) {
        let ciImage = CIImage(cvPixelBuffer: imageBuffer)
        let context = CIContext()
        if let cgImage = context.createCGImage(ciImage, from: ciImage.extent) {
            DispatchQueue.main.async {
                self.contents = cgImage
            }
        }
    }
}

struct ErrorLogEvent {
    let date: Date
    let errorComment: String
}

final class ErrorLog {
    private(set) var events: [ErrorLogEvent] = []
    
    func addEvent(_ event: ErrorLogEvent) {
        events.append(event)
    }
    
    func lastEvent() -> ErrorLogEvent? {
        return events.last
    }
    
    var allEvents: [ErrorLogEvent] {
        return events
    }
}

final class CustomHLSPlayerItem: NSObject {
    let parser: HLSParser
    
    var startsOnFirstEligibleVariant: Bool = true
    var preferredPeakBitRate: Double = 0.0
    
    @objc dynamic private(set) var playbackBufferEmpty: Bool = false
    @objc dynamic private(set) var playbackLikelyToKeepUp: Bool = true
    @objc dynamic private(set) var playbackBufferFull: Bool = true
    @objc dynamic private(set) var status: AVPlayerItem.Status = .unknown
    @objc dynamic private(set) var presentationSize: CGSize = .zero
    
    private let url: URL
    private var errorLogInternal = ErrorLog()
    private(set) var errorOccurred = false
    private(set) var segments: [HLSParser.Segment] = []
    private(set) var availableQualities: [URL] = []
    
    private var observers = [NSKeyValueObservation]()
    
    
    init(url: URL) {
        self.url = url
        self.parser = HLSParser()
        super.init()
    }
    
    func loadPlaylist(completion: @escaping (Bool) -> Void) {
        parser.loadPlaylist(from: url) { [weak self] success in
            guard success, let self = self else {
                completion(false)
                return
            }
            self.segments = parser.segments
            self.availableQualities = parser.availableQualities
            completion(true)
        }
    }
    
    func updateBufferStatus(isBufferEmpty: Bool, likelyToKeepUp: Bool, bufferFull: Bool) {
        self.playbackBufferEmpty = isBufferEmpty
        self.playbackLikelyToKeepUp = likelyToKeepUp
        self.playbackBufferFull = bufferFull
    }
    
    private func recordError(_ message: String) {
        let event = ErrorLogEvent(date: Date(), errorComment: message)
        errorLogInternal.addEvent(event)
        errorOccurred = true
        
        NotificationCenter.default.post(name: .CustomPlayerItemFailedToPlayToEnd, object: self)
    }
    
    func errorLog() -> ErrorLog? {
        return errorLogInternal
    }
    
    func notifyPlayToEnd() {
        NotificationCenter.default.post(name: .CustomPlayerItemDidPlayToEnd, object: self)
    }
    
    func notifyNewErrorEntry(_ message: String) {
        let event = ErrorLogEvent(date: Date(), errorComment: message)
        errorLogInternal.addEvent(event)
        NotificationCenter.default.post(name: .CustomPlayerItemNewErrorLogEntry, object: self)
    }
    
    deinit {
        observers.forEach { $0.invalidate() }
    }
}

extension Notification.Name {
    static let CustomPlayerItemDidPlayToEnd = Notification.Name("CustomPlayerItemDidPlayToEnd")
    static let CustomPlayerItemFailedToPlayToEnd = Notification.Name("CustomPlayerItemFailedToPlayToEnd")
    static let CustomPlayerItemNewErrorLogEntry = Notification.Name("CustomPlayerItemNewErrorLogEntry")
}

final class HLSParser {
    struct Segment {
        let url: URL
        let duration: Double
    }
    
    var segments: [Segment] = []
    var availableQualities: [URL] = []
    
    func loadPlaylist(from url: URL, completion: @escaping (Bool) -> Void) {
        URLSession.shared.dataTask(with: url) { [weak self] data, _, error in
            guard let data = data, error == nil, let self = self else {
                completion(false)
                return
            }
            
            self.parsePlaylist(data) {
                completion(true)
            }
            
        }.resume()
    }

    private func parsePlaylist(_ data: Data, completion: @escaping () -> Void) {
        DispatchQueue.global().async {
            guard let playlistContent = String(data: data, encoding: .utf8) else {
                DispatchQueue.main.async { completion() }
                return
            }
            
            let lines = playlistContent.components(separatedBy: "\n")
            var currentDuration: Double?
            
            for line in lines {
                if line.hasPrefix("#EXT-X-STREAM-INF") {
                    if let url = self.extractURL(from: line) {
                        self.availableQualities.append(url)
                    }
                } else if line.hasPrefix("#EXTINF:") {
                    currentDuration = self.extractDuration(from: line)
                } else if !line.hasPrefix("#"), let url = URL(string: line), let duration = currentDuration {
                    self.segments.append(Segment(url: url, duration: duration))
                    currentDuration = nil
                }
            }
            
            DispatchQueue.main.async {
                completion()
            }
        }
    }
    
    private func extractDuration(from line: String) -> Double? {
        let durationString = line.replacingOccurrences(of: "#EXTINF:", with: "").split(separator: ",").first
        return Double(durationString ?? "")
    }
    
    private func extractURL(from line: String) -> URL? {
        return URL(string: line)
    }
}
