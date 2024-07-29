import Foundation
import PackagePlugin

#if canImport(Darwin)
@main
struct VendorSQLcipher: CommandPlugin {
    // Constants
    static let usage = """
        SQLcipher vendoring command

        This command provides support for updating the embedded C sqlitecipher library.

        Options:
          -h, --help              Show this help.
          -v, --verbose           Output detailed progress information.
          --force                 Run the vendoring process even if the currently embedded version
                                    is already up to date.
        """
    static let sqliteURL = URL(string: "https://sqlite.org")!
    static let sqliteSourceFilePrefix = "sqlite3"
    static let vendorPrefix = "sqlite_nio"
    
    static var verbose = false
    
    var verbose: Bool { Self.verbose }
    
    func performCommand(context: PluginContext, arguments: [String]) async throws {
        var extractor = UsefulArgumentExtractor(arguments)
        
        // Handle arguments
        if extractor.extractFlag(named: "help", shortForm: "h") > 0 {
            return print(Self.usage)
        }
        
        let force = extractor.extractFlag(named: "force") > 0
        Self.verbose = extractor.extractFlag(named: "verbose", shortForm: "v") > 0

        guard extractor.remainingArguments.isEmpty else {
            for f in extractor.unextractedOptionsOrFlags { Diagnostics.error("Unknown option '\(f)'.") }
            if extractor.remainingArguments.count > extractor.unextractedOptionsOrFlags.count {
                Diagnostics.error("This command does not accept any arguments.")
            }
            return print(Self.usage)
        }
        
        // Clear work directory
        for item in try FileManager.default.contentsOfDirectory(at: context.pluginWorkDirectory.directoryUrl, includingPropertiesForKeys: nil, options: .skipsSubdirectoryDescendants) {
            try FileManager.default.removeItem(at: item)
        }
        
        // Find C target
        guard let target = try context.package.targets(named: ["CSQLcipher"]).first.flatMap({ $0 as? ClangSourceModuleTarget }) else {
            throw VendoringError("Unable to find the CSQLcipher target in package.")
        }
        if self.verbose { Diagnostics.verbose("Found CSQLcipher target with path \(target.directory)") }

        // Load current version
        guard let line = try await target.directory.appending("version.txt").fileUrl.lines.first(where: { !$0.starts(with: "//") }),
              let currentVersion = SemanticVersion(line)
        else {
            throw VendoringError("Could not read version stamp.")
        }
        if self.verbose { Diagnostics.verbose("Current version: \(currentVersion)") }
        
        // Copy pre-generated sources, apply patches, replace current sources.
        try self.copyPatch(context: context, target: target)
        
        // Extract symbol graph from new sources.
        let symbols = try await self.extractSymbols(for: target, context: context)

        // MARK: Prefix the symbols in the new sources.
        try await self.prefixFile(
            at: target.publicHeadersDirectory!.appending("\(Self.vendorPrefix)_sqlite3.h"),
            using: symbols,
            in: context
        )
        try await self.prefixFile(
            at: target.directory.appending("\(Self.vendorPrefix)_sqlite3.c"),
            using: symbols,
            in: context
        )
        
        // Stamp sources with updated version info.
        /*
        try """
        // This directory is generated from SQLite sources downloaded from \(latestData.downloadURL.absoluteString)
        \(latestData.version)
        
        """.write(to: target.directory.appending("version.txt").fileUrl, atomically: true, encoding: .utf8)
        */

        //Diagnostics.verbose("Upgraded from \(currentVersion) to \(latestData.version)")
    }
    
    private func getLatestDownloadInfo() async throws -> Sqlite3ProductInfo {
        let downloadHtmlURL = Self.sqliteURL.appendingPathComponent("download.html", isDirectory: false)
        var foundCSV = false
        
        do {
            for try await line in downloadHtmlURL.lines {
                if !foundCSV, line.hasSuffix("Download product data for scripts to read") {
                    foundCSV = true
                } else if foundCSV, !line.hasPrefix("PRODUCT,") {
                    break
                } else if foundCSV, line.contains("sqlite-amalgamation") {
                    let info = try Sqlite3ProductInfo(from: line)
                    
                    if info.filename.starts(with: "sqlite-amalgamation") {
                        return info
                    }
                }
            }
        } catch let error as URLError where error.code == .cannotFindHost {
            throw VendoringError("DNS error when trying to load downloads list. You probably need to use --disable-sandbox. Underlying error: \(error)")
        }
        
        if foundCSV {
            throw VendoringError("Could not find latest version on download page.")
        } else {
            throw VendoringError("Failed to find product CSV table on download page.")
        }
    }
    
    private func copyPatch(context: PluginContext, target: ClangSourceModuleTarget) throws {
        if self.verbose { Diagnostics.verbose("Copying files from \(context.package.directory)/src for processing.") }
        try FileManager.default.copyItem(
            at: context.package.directory.appending("src", "sqlite3.h").fileUrl,
            to: context.pluginWorkDirectory.appending("sqlite3.h").fileUrl
        )
        try FileManager.default.copyItem(
            at: context.package.directory.appending("src", "sqlite3.c").fileUrl,
            to: context.pluginWorkDirectory.appending("sqlite3.c").fileUrl
        )
        try Process.run("patch", "-\(self.verbose ? "" : "s")d", "\(context.pluginWorkDirectory)", "-p1", "-u", "-i", "\(Path(#filePath).replacingLastComponent(with: "001-warnings-and-data-race.patch"))")
        try FileManager.default.replaceItem(
            at: target.publicHeadersDirectory!.appending("\(Self.vendorPrefix)_sqlite3.h").fileUrl,
            withItemAt: context.pluginWorkDirectory.appending("sqlite3.h").fileUrl,
            backupItemName: nil, resultingItemURL: nil
        )
        try FileManager.default.replaceItem(
            at: target.directory.appending("\(Self.vendorPrefix)_sqlite3.c").fileUrl,
            withItemAt: context.pluginWorkDirectory.appending("sqlite3.c").fileUrl,
            backupItemName: nil, resultingItemURL: nil
        )
    }

    private func extractSymbols(for target: any PackagePlugin.SourceModuleTarget, context: PluginContext) async throws -> [Substring] {
        // Get a list of relevant symbols from the SPM symbol graph.
        if self.verbose { Diagnostics.verbose("Starting symbol graph generation") }
        let symbolGraphFile = try self.packageManager.getSymbolGraph(for: target, options: .init(
            minimumAccessLevel: .public, includeSynthesized: false, includeSPI: false, emitExtensionBlocks: false
        )).directoryPath.appending("\(target.name).symbols.json")
        
        let symbolGraph = try JSONDecoder().decode(SymbolGraph.self, from: Data(contentsOf: symbolGraphFile.fileUrl))
        
        let graphSymbols = Set(symbolGraph.symbols.compactMap {
            ($0.kind.identifier == "swift.func" ? $0.identifier.precise.dropFirst("c:@F@".count) :
            ($0.kind.identifier == "swift.var" && !$0.identifier.precise.contains("@macro@") ?
                $0.identifier.precise.dropFirst("c:@".count) :
            nil))
        })
        if self.verbose { Diagnostics.verbose("Found \(graphSymbols.count) symbols in the graph") }
        
        // The symbol graph can only handle symbols that ClangImporter is able to import into Swift, which excludes
        // functions that use C variadic args like sqlite3_config(), so use nm to extract a symbol list from the
        // generated object file(s) as well.
        if self.verbose { Diagnostics.verbose("Starting object file generation") }
        guard try self.packageManager.build(.target(target.name), parameters: .init()).succeeded else {
            throw VendoringError("Build command failed (unspecified reason)")
        }

        let objDir = context.package.directory.appending(".build", "debug", "\(target.name).build")
        var objSymbols: Set<Substring> = []
        for object in try FileManager.default.contentsOfDirectory(at: objDir.directoryUrl, includingPropertiesForKeys: nil)
            .filter({ $0.pathExtension == "o" })
        {
            objSymbols.formUnion(try await Process.popen("nm", "-gUj", object.path).split(separator: "\n").map { $0.dropFirst() })
        }
        if self.verbose { Diagnostics.verbose("Got \(objSymbols.count) symbols from object files")}
        
        // It turns out that both the symbol graph and the object files have symbols that the other doesn't, so we
        // take the union of both.
        let allSymbols = graphSymbols.union(objSymbols).sorted()
        if self.verbose { Diagnostics.verbose("Loaded \(allSymbols.count) unique symbols from the graph and objects") }
        
        // Remove symbols that have a common prefix matching entire shorter symbol names. This both prevents multiple
        // prefixing of symbols and cuts down on the number of replacements we do per input line.
        let commonPrefixSymbols = allSymbols.reduce(into: [Substring]()) { res, sym in
            if res.last.map({ !sym.starts(with: $0) }) ?? true { res.append(sym) }
        }
        if self.verbose { Diagnostics.verbose("\(allSymbols.count - commonPrefixSymbols.count) symbols had common prefixes") }
        
        return commonPrefixSymbols
    }
    
    private func prefixFile(at file: Path, using symbols: [Substring], in context: PluginContext) async throws {
        do { // Make sure the file handles are closed before we move the output into place.
            let reader = try FileHandle(forReadingFrom: file.fileUrl)
            defer { try? reader.close() }
            
            let outputFile = context.pluginWorkDirectory.appending(file.lastComponent)
            // `FileHandle(forWritingTo:)` refuses to create new files.
            FileManager.default.createFile(atPath: outputFile.string, contents: nil)
            let writer = try FileHandle(forWritingTo: outputFile.fileUrl)
            defer { try? writer.close() }
            
            // Filter out SQLcipher-added symbols to keep text substitution simple.
            let symbs: [Substring] = symbols.filter { !$0.contains("sqlcipher") }
            // We know what prefix we want.
            let minimalCommonPrefix = "sqlite3_"
            Diagnostics.verbose("Prefixing symbols in \(file.lastComponent) (minimum prefix \(minimalCommonPrefix))")

            for try await line in reader.bytes.keepingEmptySubsequencesLines {
                let oline = line.contains(minimalCommonPrefix) ?
                    symbs.reduce(line, { $0.replacingOccurrences(of: $1, with: "\(Self.vendorPrefix)_\($1)") }) :
                    line
                try writer.write(contentsOf: Array("\(oline)\n".utf8))
            }
        }
        
        try FileManager.default.replaceItem(
            at: file.fileUrl,
            withItemAt: context.pluginWorkDirectory.appending(file.lastComponent).fileUrl,
            backupItemName: nil, resultingItemURL: nil
        )
    }
}

extension Diagnostics {
    static func verbose(_ message: String) {
        // Diagnostics.remark() only shows up if SPM itself is in verbose mode, which is probably noisier than desired.
        print("verbose: \(message)")
    }
}
#else
@main
struct VendorSQLcipher: CommandPlugin {
    func performCommand(context: PluginContext, arguments: [String]) async throws {
        Diagnostics.error("This plugin is not implemented on non-Apple platforms.")
        throw VendoringError("This plugin is not implemented on non-Apple platforms.")
    }
}
#endif

struct VendoringError: Error, ExpressibleByStringLiteral, CustomStringConvertible {
    let description: String
    
    init(stringLiteral value: String) { self.description = value }
    init(_ description: String) { self.description = description }
}

